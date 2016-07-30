var Decoder = require('blob-log-decoder')
var asyncQueue = require('async.queue')
var crcHash = require('crc-hash').createHash
var fs = require('fs')
var lexi = require('lexicographic-integer')
var mkdirp = require('mkdirp')
var path = require('path')
var pump = require('pump')
var runSeries = require('run-series')
var through2 = require('through2')

var CRC_BYTES = 4
var LENGTH_BYTES = 4

module.exports = BlobLog

function BlobLog (options) {
  if (!(this instanceof BlobLog)) {
    return new BlobLog(options)
  }

  var log = this
  log._blobsPerFile = options.blobsPerFile || 100
  log._directory = options.directory || '.blob-log'
  log.length = 0

  log._queue = asyncQueue(function (task, done) {
    if (task.buffer) {
      log._appendBuffer(task.buffer, function (error) {
        task.callback(error)
        done()
      })
    } else {
      throw new Error('not implemented')
    }
  })

  mkdirp(log._directory, function (error) {
    if (error) throw error
  })
}

var prototype = BlobLog.prototype

prototype._fileForIndex = function (index) {
  return Math.floor(index / this._blobsPerFile)
}

prototype._filePathFor = function (number) {
  return path.join(this._directory, lexi.pack(number, 'hex'))
}

prototype.appendBuffer = function (buffer, callback) {
  this._queue.push({
    buffer: buffer,
    callback: callback
  })
}

prototype._appendBuffer = function (buffer, callback) {
  var log = this
  var index = log.length
  var fileNumber = log._fileForIndex(index)
  var filePath = log._filePathFor(fileNumber)
  var firstBlobInFile = (index % this._blobsPerFile) === 0
  var firstNumberBytes = firstBlobInFile ? 4 : 0
  var crc = createCRC()
  .update(buffer)
  .digest()
  var blob = new Buffer(
    firstNumberBytes + // First blob number
    4 + // Blob length
    4 + // CRC-32
    buffer.length // Blob
  )
  if (firstNumberBytes !== 0) {
    blob.writeUInt32BE(index, 0)
  }
  blob.writeUInt32BE(buffer.length, firstNumberBytes + 0)
  crc.copy(buffer, firstNumberBytes + 4)
  buffer.copy(blob, firstNumberBytes + 8)
  fs.appendFile(filePath, blob, function (error) {
    if (error) {
      callback(error)
    } else {
      log.length++
      callback()
    }
  })
}

prototype._appendStream = function (dataStream, callback) {
  var log = this
  var index = log.length
  var fileNumber = log._fileForIndex(index)
  var filePath = log._filePathFor(fileNumber)

  var fileLength
  var streamLength = 0
  var crc = createCRC()

  runSeries([getLength, streamData, rewritePrefix], function (error) {
    if (error) callback(error)
    else callback(index)
  })

  function getLength (callback) {
    // If this index is the first blob in the file, write the index
    // number to the start of the file.
    if (index % log._blobsPerFile === 0) {
      fileLength = 4
      var writeStream = createWriteStream()
      .once('error', function (error) {
        callback(error)
      })
      .once('finish', function () {
        callback()
      })
      writeStream.writeUInt32BE(index)
    // Otherwise, stat for the current file length.
    } else {
      fs.stat(filePath, function (error, stats) {
        if (error) callback(error)
        else {
          fileLength = stats.length
          callback()
        }
      })
    }
  }

  function streamData (callback) {
    var writeStream = createWriteStream()
    writeStream.write(new Buffer(LENGTH_BYTES).fill(0))
    writeStream.write(new Buffer(CRC_BYTES).fill(0))
    pump(
      dataStream,
      through2(function (chunk, encoding, done) {
        streamLength += chunk.length
        crc.update(chunk)
        done(null, chunk)
      }),
      writeStream
    )
    .once('error', callback)
    .once('finish', callback)
  }

  function rewritePrefix () {
    var writeStream = createWriteStream()
    .once('error', callback)
    .once('finish', callback)
    writeStream.writeUInt32BE(streamLength)
    writeStream.end(crc.digest())
  }

  function createWriteStream () {
    return fs.createWriteStream(filePath, {start: fileLength})
  }
}

prototype.createReadStream = function (from) {
  var log = this
  if (from === undefined) {
    from = 0
  }
  var length = log.length
  if (length === 0) {
    return false
  } else {
    var throughStream = through2.obj()
    var lastFile = log._fileForIndex(length - 1)
    var fileNumber = log._fileForIndex(from)
    pipeNextFile()
    return throughStream
  }
  function pipeNextFile () {
    var filePath = log._filePathFor(fileNumber)
    var fileReadStream = fs.createReadStream(filePath)
    var decoder = pump(fileReadStream, new Decoder())
    if (fileNumber === lastFile) {
      decoder.pipe(throughStream, {end: true})
    } else {
      decoder
      .once('end', function () {
        fileNumber++
        pipeNextFile()
      })
      .pipe(throughStream, {end: false})
    }
  }
}

function createCRC () {
  return crcHash('crc32')
}
