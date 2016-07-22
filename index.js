var BlockStream = require('block-stream')
var fs = require('fs')
var lexi = require('lexicographic-integer')
var mkdirp = require('mkdirp')
var path = require('path')
var pump = require('pump')
var through2 = require('through2')

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

prototype.write = function (hash, callback) {
  if (hash.length !== this._hashLength) {
    throw new Error('wrong-length hash')
  }
  var log = this
  var fileNumber = log._fileForIndex(log.length)
  var filePath = log._filePathFor(fileNumber)
  fs.appendFile(filePath, hash, 'utf8', function (error) {
    if (error) {
      callback(error)
    } else {
      log.length++
      callback()
    }
  })
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
