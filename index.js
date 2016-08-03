var Decoder = require('blob-log-decoder')
var Encoder = require('blob-log-encoder')
var EventEmitter = require('events').EventEmitter
var MultiStream = require('multistream')
var MultiWritable = require('multiwritable')
var arrayFind = require('array-find')
var fs = require('fs')
var inherits = require('util').inherits
var lexint = require('lexicographic-integer')
var mkdirp = require('mkdirp')
var path = require('path')
var pump = require('pump')
var runSeries = require('run-series')

module.exports = BlobLog

var constructorOptions = {
  directory: nonEmptyString,
  blobsPerFile: optional(possiblyInfinite(positiveInteger))
}

function BlobLog (options) {
  if (!(this instanceof BlobLog)) {
    return new BlobLog(options)
  }
  var self = this

  // Save options.
  validateOptions(constructorOptions, options)
  self._blobsPerFile = options.blobsPerFile || 10
  self._directory = options.directory
  self._index = null

  EventEmitter.call(this)

  // Initialize.
  runSeries([
    mkdirp.bind(null, this._directory),
    this._readExistingFiles.bind(this),
    this._checkTailFile.bind(this)
  ], function (error) {
    if (error) {
      self.emit('error', error)
    } else {
      self.emit('ready')
    }
  })
}

inherits(BlobLog, EventEmitter)

var prototype = BlobLog.prototype

// Initialization Methods

prototype._readExistingFiles = function (callback) {
  var self = this
  var directory = self._directory
  // Get a list of file names in the directory, then filter out
  // and sort lexicographic-integer file names, to determine where
  // to start writing new blobs.
  fs.readdir(directory, function (error, files) {
    /* istanbul ignore if */
    if (error) {
      callback(error)
    } else {
      // Compile a sorted list of the integer values that
      // correspond to existing blob log files.
      var fileNumbers = []
      files.forEach(function (file) {
        var number = logFileNumber(file)
        if (number !== undefined) {
          fileNumbers.push(number)
        }
      })
      fileNumbers.sort()
      // Do we have a continous set of log file numbers, without
      // any gaps?
      var missing = arrayFind(fileNumbers, function (element, index) {
        return element !== index + 1
      })
      if (missing) {
        var missingPath = self._filePathForIndex(missing - 1)
        callback(new Error('missing ' + missingPath))
      } else {
        // Store the number of the latest log file.
        self._fileNumber = fileNumbers[fileNumbers.length - 1]
        callback()
      }
    }
  })
}

prototype._checkTailFile = function (callback) {
  var self = this
  if (self._fileNumber === undefined) {
    self._blobsInFile = 0
    self._index = 0
    callback()
  } else {
    var lastIndex = 0
    var blobCount = 0
    fs.createReadStream(self._tailFilePath())
    .once('error', /* istanbul ignore next */ function (error) {
      if (error.code === 'ENOENT') {
        self._blobsInFile = 0
        self._index = 0
        callback()
      } else {
        callback(error)
      }
    })
    .pipe(
      new Decoder()
      .once('error', /* istanbul ignore next */ function (error) {
        callback(error)
      })
      .on('data', function (blob) {
        lastIndex = blob.index
        blobCount++
      })
      .once('end', function () {
        self._blobsInFile = blobCount
        self._index = lastIndex
        callback()
      })
    )
  }
}

// Log File Helper Methods

var LOG_FILE_EXTENSION = '.bloblog'

function logFileNumber (file) {
  var extension = path.extname(file)
  if (extension === LOG_FILE_EXTENSION) {
    return unpackInteger(path.basename(file, extension))
  }
}

prototype._filePathForIndex = function (number) {
  return path.join(
    this._directory,
    packInteger(number) + LOG_FILE_EXTENSION
  )
}

prototype._tailFilePath = function () {
  return this._filePathForIndex(this._fileNumber)
}

/* istanbul ignore next: TODO */
prototype._fileNumberForIndex = function (index) {
  return Math.floor((index - 1) / this._blobsPerFile) + 1
}

/* istanbul ignore next: TODO */
prototype._nthInFile = function (index) {
  return (index - 1) % this._blobsPerFile + 1
}

/* istanbul ignore next: TODO */
prototype._firstInFile = function (index) {
  return this._nthInFile(index) === 1
}

/* istanbul ignore next: TODO */
prototype._lastInFile = function (index) {
  return this._nthInFile(index) === this._blobsPerFile
}

// Public API

prototype.createWriteStream = function () {
  var self = this
  var options = {
    end: true,
    objectMode: true
  }
  function sinkFactory (currentSink, chunk, encoding, callback) {
    var index = self._index + 1
    var file = self._filePathForIndex(index)
    if (currentSink && currentSink.path === file) {
      callback(null, currentSink)
    } else {
      var encoder = new Encoder(index)
      encoder.path = file
      var writeStream = fs.createWriteStream(file)
      pump(encoder, writeStream)
      callback(null, encoder)
    }
  }
  self._internalWriteStream = MultiWritable(sinkFactory, options)
  return self._internalWriteStream
}

// TODO: Options object with from index
prototype.createReadStream = function () {
  var self = this
  var fileIndex = 0
  return MultiStream.obj(function (callback) {
    fileIndex++
    if (fileIndex > self._fileNumber) {
      callback(null, null)
    } else {
      var filePath = self._filePathForIndex(fileIndex)
      var readStream = fs.createReadStream(filePath)
      var decoder = new Decoder()
      pump(readStream, decoder)
      callback(null, decoder)
    }
  })
}

// Options Validation

function validateOptions (predicates, options) {
  if (typeof options.directory !== 'string') {
    throw new Error('missing directory')
  }
  Object.keys(predicates).forEach(function (key) {
    if (!predicates[key](options[key])) {
      throw new Error('invalid ' + key)
    }
  })
}

function optional (predicate) {
  return function (argument) {
    return argument === undefined || predicate(argument)
  }
}

function possiblyInfinite (predicate) {
  return function (argument) {
    return argument === Infinity || predicate(argument)
  }
}

function nonEmptyString (argument) {
  return typeof argument === 'string' && argument.length !== 0
}

function positiveInteger (argument) {
  return Number.isInteger(argument) && argument > 0
}

// Integer Packing

function packInteger (integer) {
  return lexint.pack(integer, 'hex')
}

function unpackInteger (string) {
  return lexint.unpack(string, 'hex')
}
