var BlobLog = require('./')
var asyncMap = require('async.map')
var concatStream = require('concat-stream')
var mktempd = require('temporary-directory')
var tape = require('tape')

tape('first', function (test) {
  mktempd(function (error, directory, cleanUp) {
    test.ifError(error)

    var log = new BlobLog({
      directory: directory,
      blobsPerFile: 2
    })

    var data = []
    while (data.length < 100) {
      data.push(new Buffer('Blob ' + data.length.toString(), 'utf8'))
    }

    asyncMap(data, append, function (error) {
      test.ifError(error, 'no error')
      var read = []
      log.createReadStream()
      .once('error', function (error) {
        test.ifError(error, 'no error')
      })
      .on('data', function (blob) {
        read.push(blob)
      })
      .once('end', function () {
        test.equal(
          read.length, data.length,
          'read ' + data.length + ' blogs'
        )
        asyncMap(read, bufferBlob, function (error, results) {
          test.ifError(error, 'no error')
          test.deepEqual(
            results.map(function (element) {
              return element.string
            }),
            data.map(function (element) {
              return element.toString()
            }),
            'strings'
          )
          cleanUp()
          test.end()
        })
        function bufferBlob (read, done) {
          read.stream.pipe(concatStream(function (buffered) {
            done(null, {
              index: read.index,
              string: buffered.toString()
            })
          }))
        }
      })
    })

    function append (buffer, done) {
      log.appendBuffer(buffer, done)
    }
  })
})
