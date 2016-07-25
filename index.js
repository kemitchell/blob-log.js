var asyncQueue = require('async.queue')
var fs = require('fs')
var lexint = require('lexicographic-integer')
var lps = require('length-prefixed-stream')
var mkdirp = require('mkdirp')
var path = require('path')
var pump = require('pump')
var runSeries = require('run-series')
var varint = require('varint')

module.exports = BlobLog

function BlobLog (options) {
  if (!(this instanceof BlobLog)) {
    return new BlobLog(options)
  }
  var self = this

  self._blobsPerFile = options.blobsPerFile
  self._directory = options.directory
  var checkFunction = self._checkFunction = options.checkFunction
  self._checkBytes = options.checkBytes

  self._head = undefined
  self._writeStream = undefined

  self._appendQueue = asyncQueue(function (task, done) {
    var blob = task.blob
    var callback = task.callback
    var next = self._head + 1
    // Create a chunk like:
    // +--------+-------+------+
    // | varint | check | blob |
    // +--------+-------+------+
    // The varint's length varies.
    // Check is `self._checkBytes` long.
    // The blob's length varies.
    var check = checkFunction(blob)
    var checkAndBlob = Buffer.concat([check, blob])
    var varintBytes = varint.encodingLength(next)
    var chunk = new Buffer(varintBytes + checkAndBlob.length)
    varint.encode(next, chunk)
    checkAndBlob.copy(chunk, varintBytes)
    // Write to file.
    var filePath = self._filePathForIndex(next)
    fs.appendFile(filePath, chunk, function (error) {
      if (error) {
        callback(error)
        done()
      } else {
        self._head = next
        callback()
        done()
      }
    })
  }, 1 /* one concurrent worker */)

  // Pause the work queue until `initialize()` sets `self._head` and
  // `self._writeStream`.
  self._appendQueue.pause()
}

var prototype = BlobLog.prototype

// Create the directory if it doesn't exist. If it does exist, read
// enough information to find out what the current head index is.
prototype.initialize = function (callback) {
  var self = this
  var directory = self._directory
  var lastFile = null

  fs.stat(directory, function (error, stats) {
    if (error) {
      if (error.code === 'ENOENT') {
        console.log('making dir')
        mkdirp(directory, function (error) {
          if (error) {
            callback(error)
          } else {
            self._head = 0
            callback()
          }
        })
      } else {
        callback(error)
      }
    } else {
      if (!stats.isDirectory()) {
        callback(new Error(directory + ' is not a directory'))
      } else {
        runSeries([findLastFile, findHead, resumeAppendQueue], callback)
      }
    }
  })

  // List files in the directory, decode them, and find the
  // highest-numbered.
  function findLastFile (done) {
    fs.readdir(directory, function (error, files) {
      if (error) {
        done(error)
      } else {
        if (files.length === 0) {
          lastFile = 0
        } else {
          var numbers = files.map(unpack).sort()
          lastFile = numbers[numbers.length - 1]
        }
        done()
      }
    })
  }

  // Read the highest-numbered file in the directory and count how many
  // blobs it already has. Set `self._head` and set up a write stream
  // for new blobs.
  function findHead (done) {
    if (lastFile === 0) {
      self._head = 0
      done()
    } else {
      var file = path.join(directory, pack(lastFile))
      var blobsInLastFile = 0
      pump(
        fs.createReadStream(file),
        lps.decode()
      )
      .once('error', function (error) {
        done(error)
      })
      .on('data', function () {
        blobsInLastFile++
      })
      .on('end', function () {
        var beforeLastFile = ((lastFile - 1) * self._blobsPerFile)
        self._head = beforeLastFile + blobsInLastFile
      })
    }
  }

  function resumeAppendQueue (done) {
    self._appendQueue.resume()
    done()
  }
}

prototype.append = function (blob, callback) {
  this._appendQueue.push({blob: blob, callback: callback})
}

prototype.createReadStream = function (from) {
  
}

prototype.head = function () {
  return this._head
}

prototype._fileNumberForIndex = function (index) {
  return Math.floor(index / this._blobsPerFile)
}

prototype._filePathForIndex = function (index) {
  return path.join(
    this._directory,
    pack(this._fileNumberForIndex(index))
  )
}

function pack (argument) {
  return lexint.pack(argument, 'hex')
}

function unpack (argument) {
  return lexint.unpack(argument, 'hex')
}
