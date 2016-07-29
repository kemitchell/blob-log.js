var BlockStream = require('block-stream')
var crcHash = require('crc-hash')
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

  this._hashLength = options.hashLength || 64
  this._hashesPerFile = options.hashesPerFile || 10000
  this._directory = options.directory || '.blob-log'
  this.length = 0
  mkdirp(this._directory, function (error) {
    if (error) {
      throw error
    }
  })
}

var prototype = BlobLog.prototype

prototype._makeBuffer = function () {
  return new ArrayBuffer(this._bufferSize)
}

prototype._fileForIndex = function (index) {
  return Math.floor(index / this._hashesPerFile)
}

prototype._filePathFor = function (number) {
  return path.join(this._directory, lexi.pack(number, 'hex'))
}

prototype._currentfile = function () {
  return this._fileForIndex(this.length)
}

prototype._appendBuffer = function (buffer, callback) {
  var log = this
  var index = log.length
  var fileNumber = log._fileForIndex(index)
  var filePath = log._filePathFor(fileNumber)
  var lengthPrefix = new Buffer(LENGTH_BYTES)
  lengthPrefix.writeUInt32BE(buffer.byteLength)
  var crc = createCRC()
  .update(buffer)
  .digest()
  var blob = Buffer.concat([lengthPrefix, crc, buffer])
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
    fs.stat(filePath, function (error, stats) {
      if (error) callback(error)
      else {
        fileLength = stats.length
        callback()
      }
    })
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

prototype._createReadStream = function (file) {
  var readStream = fs.createReadStream(file)
  var returned = through2.obj(function (chunk, encoding, done) {
    
  })
  return pump(readStream, returned)
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
    var blockStream = new BlockStream(log._hashLength)
    var throughStream = through2.obj(function (chunk, _, done) {
      done(null, chunk.toString())
    })
    pump(blockStream, throughStream)
    var lastFile = log._fileForIndex(length - 1)
    var fileNumber = log._fileForIndex(from)
    var offset = (from % log._hashesPerFile) * log._hashLength
    pipeNextFile()
    return throughStream
  }
  function pipeNextFile () {
    var filePath = log._filePathFor(fileNumber)
    var fileReadStream = fs.createReadStream(filePath, {start: offset})
    .once('error', function (error) {
      throughStream.emit('error', error)
    })
    if (fileNumber === lastFile) {
      fileReadStream
      .pipe(blockStream)
    } else {
      fileReadStream
      .once('end', function () {
        fileNumber++
        offset = 0
        pipeNextFile()
      })
      .pipe(blockStream, {end: false})
    }
  }
}

function createCRC () {
  return crcHash('CRC-32')
}
