var Decoder = require('blob-log-decoder')
var Encoder = require('blob-log-encoder')
var Multistream = require('multistream')
var StreamingAppend = require('stream-to-blob-log')
var fs = require('fs')
var lexint = require('lexicographic-integer')
var path = require('path')
var pump = require('pump')
var runSeries = require('run-series')

module.exports = BlobLog

var constructorOptions = {
  directory: nonEmptyString,
  blobsPerFile: optional(possiblyInfinite(positiveInteger)),
  blobsPerBatch: optional(positiveInteger)
}

var LOG_FILE_EXTENSION = 'bloblog'
var LOG_FILE_RE = new RegExp('^([^.]+)\\.' + LOG_FILE_EXTENSION + '$')

function BlobLog (options) {
  if (!(this instanceof BlobLog)) {
    return new BlobLog(options)
  }
  var self = this

  // Save options.
  validateOptions(constructorOptions, options)
  self._blobsPerBatch = options.blobsPerBatch || 1
  self._blobsPerFile = options.blobsPerFile || 10
  self._directory = options.directory

  // Initialize.
  runSeries([
    this._validateDirectory.bind(this),
    this._readExistingFiles.bind(this),
    this._checkCurrentFile.bind(this)
  ], function (error) {
    self.emit('error', error)
  })
}

var prototype = BlobLog.prototype

// Initialization Methods

prototype._validateDirectory = function (callback) {
  var self = this
  var directory = self._directory
  // `stat` the directory path, to make sure it's a directory.
  fs.stat(directory, function (error, stats) {
    if (error) {
      callback(error)
    } else {
      if (!stats.isDirectory()) {
        callback(new Error('not a directory'))
      } else {
        callback()
      }
    }
  })
}

prototype._readExistingFiles = function (callback) {
  var self = this
  var directory = self._directory
  // Get a list of file names in the directory, then filter out
  // and sort lexicographic-integer file names, to determine where
  // to start writing new blobs.
  fs.readdir(directory, function (error, files) {
    if (error) {
      callback(error)
    } else {
      // Compile a sorted list of the integer values that
      // correspond to existing blob log files.
      var fileNumbers = []
      files.forEach(function (file) {
        var match = LOG_FILE_RE.exec(file)
        if (match) {
          var unpacked = unpackInteger(match[1])
          if (unpacked !== undefined) {
            fileNumbers.push(unpacked)
          }
        }
      })
      fileNumbers.sort()
      // Do we have a continous set of log file numbers, without
      // any gaps?
      var missing = fileNumbers.find(function (element, index) {
        return element !== index - 1
      })
      if (missing) {
        var missingPath = self._filePath(missing - 1)
        callback(new Error('missing ' + missingPath))
      }
      // Store the numbef of the latest log file.
      self._fileNumber = fileNumbers[fileNumbers.length - 1]
      callback()
    }
  })
}

prototype._checkCurrentFile = function (callback) {
  var self = this
  var lastIndex = 0
  var blobCount = 0
  fs.createReadStream(self._currentFile())
  .pipe(
    new Decoder()
    .once('error', function (error) {
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

prototype._filePath = function (number) {
  return path.join(
    this._directory,
    packInteger(number) + '.' + LOG_FILE_EXTENSION
  )
}

prototype._currentFile = function () {
  return this._filePath(this._fileNumber)
}

// Public API

prototype.createWriteStream = function (options) {
  if (options.objectMode) {
    
  } else {
  }
}

// TODO: Options object with from index
prototype.createReadStream = function () {
  var self = this
  var fileIndex = 0
  return new Multistream(function (callback) {
    fileIndex++
    if (fileIndex > self._fileNumber) {
      callback(null, null)
    } else {
      var readStream = fs.createReadStream(self._filePath(fileIndex))
      callback(null, pump(readStream, new Decoder()))
    }
  })
}

// Options Validation

function validateOptions (predicates, options) {
  if (typeof options.directory !== 'string') {
    throw new Error('missing directory')
  }
  Object.keys(predicates).forEach(function (key) {
    if (!predicates[key](options.key)) {
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
