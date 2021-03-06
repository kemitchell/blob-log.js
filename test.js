var BlobLog = require('./')
var Encoder = require('blob-log-encoder')
var asyncMap = require('async.map')
var concatStream = require('concat-stream')
var from2Array = require('from2-array')
var fs = require('fs')
var mktempd = require('temporary-directory')
var path = require('path')
var rimraf = require('rimraf')
var runSeries = require('run-series')
var tape = require('tape')
var touch = require('touch')

tape('round trip', function (test) {
  // Create a temporary directory to store files for this test.
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    // A few very small strings to store as blobs.
    var data = ['apple', 'banana', 'cantaloupe']
    // Store blobs in our temporary directory, two per log file.  Note
    // that applications will probably want to store far more than two
    // blobs per log file.  We use a low number in tests just to
    // exercise file spanning logic.
    var log = new BlobLog({
      directory: directory,
      blobsPerFile: 2
    })
    // Create a source `Readable` stream that emits our blobs, converted
    // to `Buffer` ofjects.
    from2Array.obj(data.map(function (string) {
      return new Buffer(string, 'ascii')
    }))
    // Pipe it to a new `Writable` stream.
    .pipe(
      log.createWriteStream()
      .once('finish', function (error) {
        test.ifError(error, 'no error')
        // Wait a bit, to give the data a chance to flush to disk.
        setTimeout(function () {
          // Read blobs back out of the `BlobLog`.
          var read = []
          log.createReadStream()
          .once('error', /* istanbul ignore next */ function (error) {
            test.fail(error)
            finish()
          })
          .on('data', function (blob) {
            read.push(blob)
          })
          .once('end', function () {
            // The `Readable` emits the blobs as objects with `Readable`
            // stream properties emitting blob data.  Concatenate each
            // of those streams into a single value for each blob.
            asyncMap(read, bufferBlob, function (error, blobs) {
              test.ifError(error, 'no error')
              // Test that we read the same strings, in the same order,
              // out of the `BlobLog`.
              test.deepEqual(
                blobs.map(function (buffer) {
                  return buffer.toString()
                }),
                data,
                'read blobs'
              )
              finish()
            })
          })
        }, 250)
      })
    )
    function finish () { cleanUp(function () { test.end() }) }
  })
})

tape('immediate close', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var data = ['a', 'b', 'c']
    var log = new BlobLog({
      directory: directory,
      blobsPerFile: 2
    })
    from2Array.obj(data.map(function (string) {
      return new Buffer(string, 'ascii')
    }))
    .pipe(
      log.createWriteStream()
      .once('finish', function (error) {
        test.ifError(error, 'no error')
        log.close()
        cleanUp(function () { test.end() })
      })
    )
  })
})

tape('delayed close', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var data = ['a', 'b', 'c']
    var log = new BlobLog({
      directory: directory,
      blobsPerFile: 2
    })
    from2Array.obj(data.map(function (string) {
      return new Buffer(string, 'ascii')
    }))
    .pipe(
      log.createWriteStream()
      .once('finish', function (error) {
        test.ifError(error, 'no error')
        setTimeout(function () {
          log.close()
          cleanUp(function () { test.end() })
        }, 100)
      })
    )
  })
})

tape('append to existing', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var firstFile = path.join(directory, '01.bloblog')
    writeOneBlob(firstFile, function () {
      var data = ['a', 'b', 'c', 'd', 'e', 'f']
      var log = new BlobLog({
        directory: directory,
        blobsPerFile: 2
      })
      from2Array.obj(data.map(function (string) {
        return new Buffer(string, 'ascii')
      }))
      .pipe(
        log.createWriteStream()
        .once('finish', function (error) {
          test.ifError(error, 'no error')
          setTimeout(function () {
            var read = []
            log.createReadStream()
            .once('error', /* istanbul ignore next */ function (error) {
              test.fail(error)
              finish()
            })
            .on('data', function (blob) {
              read.push(blob)
            })
            .once('end', function () {
              asyncMap(read, bufferBlob, function (error, blobs) {
                test.ifError(error, 'no error')
                test.deepEqual(
                  blobs.map(function (buffer) {
                    return buffer.toString()
                  }),
                  ['first blob'].concat(data),
                  'read blobs'
                )
                finish()
              })
            })
          }, 250)
        })
      )
    })
    function finish () { cleanUp(function () { test.end() }) }
  })
})

function bufferBlob (blob, callback) {
  blob.stream.pipe(concatStream(function (buffered) {
    callback(null, buffered)
  }))
}

tape('append events', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var data = ['a', 'b', 'c']
    var lastIndex = 0
    var log = new BlobLog({
      directory: directory,
      blobsPerFile: 2
    })
    .on('append', function (index) {
      lastIndex = index
    })
    from2Array.obj(data.map(function (string) {
      return new Buffer(string, 'ascii')
    }))
    .pipe(log.createWriteStream())
    setTimeout(function () {
      test.equal(lastIndex, 3, 'index is 3')
      cleanUp(function () { test.end() })
    }, 100)
  })
})

tape('construction', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    BlobLog({directory: directory})
    .once('error', /* istanbul ignore next */ function (error) {
      test.fail(error)
    })
    .once('ready', function () {
      test.pass('ready event')
      cleanUp(function () { test.end() })
    })
  })
})

tape('construction without directory', function (test) {
  test.throws(function () {
    BlobLog({})
  }, /missing directory/)
  test.end()
})

tape('construction with invalid directory', function (test) {
  test.throws(function () {
    BlobLog({directory: ''})
  }, /invalid directory/)
  test.end()
})

tape('creates missing directory', function (test) {
  var directory = path.join(process.cwd(), 'deep', 'missing')
  BlobLog({directory: directory})
  .once('error', /* istanbul ignore next */ function (error) {
    test.fail(error)
  })
  .once('ready', function () {
    test.pass('ready event')
    fs.stat(directory, function (error, stats) {
      /* istanbul ignore if */
      if (error) {
        test.ifError(error, 'no error')
      } else {
        test.equal(stats.isDirectory(), true, 'creates directory')
      }
      rimraf('deep', function () { test.end() })
    })
  })
})

tape('error when directory is a file', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var file = path.join(directory, 'existing')
    touch(file, function (error) {
      test.ifError(error, 'no error')
      BlobLog({directory: file})
      .once('error', /* istanbul ignore next */ function (error) {
        test.equal(error.code, 'EEXIST', 'emits EEXIST error')
        cleanUp(function () { test.end() })
      })
    })
  })
})

tape('missing log file', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    runSeries([
      function (done) {
        touch(path.join(directory, '01.bloblog'), done)
      },
      function (done) {
        touch(path.join(directory, '03.bloblog'), done)
      }
    ], function (error) {
      test.ifError(error, 'no error')
      BlobLog({directory: directory})
      .once('error', function (error) {
        test.equal(
          error.message,
          'missing ' + path.join(directory, '02.bloblog'),
          'emits error'
        )
        cleanUp(function () { test.end() })
      })
    })
  })
})

tape('ignores extraneous file', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    runSeries([
      function (done) {
        writeOneBlob(path.join(directory, '01.bloblog'), done)
      },
      function (done) {
        touch(path.join(directory, 'something-else'), done)
      }
    ], function (error) {
      test.ifError(error, 'no error')
      BlobLog({directory: directory})
      .once('error', /* istanbul ignore next */ function (error) {
        test.fail(error)
        finish()
      })
      .once('ready', function () {
        test.pass('no error')
        finish()
      })
    })
    function finish () { cleanUp(function () { test.end() }) }
  })
})

tape('read directory', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var log = BlobLog({directory: directory})
    .once('ready', function () {
      test.equal(log.directory(), directory, 'returns directory')
      cleanUp(function () { test.end() })
    })
  })
})

tape('read files list', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var firstFile = path.join(directory, '01.bloblog')
    writeOneBlob(firstFile, function (error) {
      test.ifError(error, 'no error')
      var log = BlobLog({directory: directory})
      .once('ready', function () {
        test.deepEqual(log.files(), [firstFile], 'returns array')
        cleanUp(function () { test.end() })
      })
    })
  })
})

tape('read length', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error, 'no error')
    var log = BlobLog({directory: directory})
    .once('ready', function () {
      log.createWriteStream()
      .once('finish', function () {
        test.equal(log.length(), 1, 'length of 1')
        cleanUp(function () { test.end() })
      })
      .end('test blob')
    })
  })
})

function writeOneBlob (file, callback) {
  var encoder = new Encoder(1)
  encoder.pipe(fs.createWriteStream(file))
  encoder.end(new Buffer('first blob', 'ascii'), callback)
}
