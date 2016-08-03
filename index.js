var BlobLogDecoder = require('blob-log-decoder')
var BlobLogEncoder = require('blob-log-encoder')
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
var through2 = require('through2')

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

  // Initialize.
  EventEmitter.call(self)

  self._length = null
  self._writeBuffer = through2.obj()

  runSeries([
    mkdirp.bind(null, self._directory),
    self._checkExistingLogFiles.bind(self),
    self._checkTailFile.bind(self),
    self._createInternalWriteStream.bind(self)
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

// Read the directory and decode file names, checking for gaps.
prototype._checkExistingLogFiles = function (callback) {
  var self = this
  var directory = self._directory
  // Get a list of file names in the directory, then filter out and sort
  // files that match the log file naming convention.
  fs.readdir(directory, function (error, files) {
    /* istanbul ignore if */
    if (error) {
      callback(error)
    } else {
      // Compile a sorted list of the integer values that correspond to
      // existing blob log files.
      var fileNumbers = []
      files.forEach(function (file) {
        var number = logFileNumber(file)
        if (number !== undefined) {
          fileNumbers.push(number)
        }
      })
      fileNumbers.sort()
      // Are there any gaps in the list of log file numbers. For
      // example: `[1, 2, 4, 5, ...]` is missing `3`.
      var missing = arrayFind(fileNumbers, function (element, index) {
        return element !== index + 1
      })
      if (missing) {
        var missingPath = self._fileNumberToPath(missing - 1)
        callback(new Error('missing ' + missingPath))
      } else {
        // Store the number of the highest-numbered ("tail") log file.
        self._tailFileNumber = fileNumbers[fileNumbers.length - 1]
        callback()
      }
    }
  })
}

// Read any existing tail log file to determine what index numbers it
// contains and how many blobs it contains.
prototype._checkTailFile = function (callback) {
  var self = this
  // The directory does not have a tail log file.
  if (self._tailFileNumber === undefined) {
    self._blobsInTailFile = 0
    self._length = 0
    callback()
  // The directory has a tail log file.
  } else {
    var lastIndex = 0
    var blobCount = 0
    fs.createReadStream(self._tailFilePath())
    .once('error', /* istanbul ignore next */ function (error) {
      if (error.code === 'ENOENT') {
        self._blobsInTailFile = 0
        self._length = 0
        callback()
      } else {
        callback(error)
      }
    })
    .pipe(
      new BlobLogDecoder()
      .once('error', /* istanbul ignore next */ function (error) {
        callback(error)
      })
      .on('data', function (blob) {
        lastIndex = blob.index
        blobCount++
      })
      .once('end', function () {
        self._blobsInTailFile = blobCount
        self._length = lastIndex
        callback()
      })
    )
  }
}

// Create a `Writable` stream that encodes blobs and writes them to
// successively numbered log files.  Then pipe the buffering stream
// created before we started checking for existing log files in the
// directory up to the new, blob-encoding `Writable`.
prototype._createInternalWriteStream = function (callback) {
  var self = this
  // Generate a succession of `BlobLogEncoder` streams piped to file
  // write streams, ensuring:
  //
  // 1. `self._blobsPerFile` blobs are written to each file
  //
  // 2. Log files are named numerically, with packed integer basenames
  //    and `LOG_FILE_EXTENSION` extensions.
  //
  // 3. Append to the existing tail file, if one already exists.
  function sinkFactory (currentSink, chunk, encoding, callback) {
    var index = self._length + 1
    self._length = index
    var fileNumber = self._blobIndexToFileNumber(index)
    self._tailFileNumber = fileNumber
    var filePath = self._fileNumberToPath(fileNumber)
    var firstBlobInFile = self._firstBlobInFile(index)
    // This blob belongs in the same file we are already writing to.
    if (currentSink && currentSink.path === filePath) {
      self._blobsInTailFile++
      callback(null, currentSink)
    // This blob belongs in a file we haven't written to yet.  Either:
    //
    // 1. This `BlobLog` was just initialized.  If there is room at the
    //    end of the existing tail log file, we should append there.
    //    Otherwise, we should start a new file.
    //
    // 2. This blob belongs in a new file we should create.
    } else {
      // If we are appending to an existing file, do not write a first
      // sequence number.
      var encoder = new BlobLogEncoder(
        firstBlobInFile ? index : undefined
      )
      encoder.path = filePath
      var writeStream = fs.createWriteStream(filePath, {flags: 'a'})
      if (firstBlobInFile) {
        self._blobsInTailFile = 1
      } else {
        self._blobsInTailFile++
      }
      pump(encoder, writeStream)
      callback(null, encoder)
    }
    self.emit('append', index)
  }
  self._writeStream = MultiWritable(sinkFactory, {
    end: true,
    objectMode: true
  })
  self._writeBuffer.pipe(self._writeStream)
  callback()
}

// Log File Helper Methods

var LOG_FILE_EXTENSION = '.bloblog'

// Given a file path like `01.bloblog`, returns `Number(1)`.
// Given a file path like `other-file.txt`, returns `undefined`.
function logFileNumber (file) {
  var extension = path.extname(file)
  if (extension === LOG_FILE_EXTENSION) {
    return unpackInteger(path.basename(file, extension))
  }
}

prototype._fileNumberToPath = function (number) {
  return path.join(
    this._directory,
    packInteger(number) + LOG_FILE_EXTENSION
  )
}

prototype._tailFilePath = function () {
  return this._fileNumberToPath(this._tailFileNumber)
}

prototype._blobIndexToFileNumber = function (index) {
  return Math.floor((index - 1) / this._blobsPerFile) + 1
}

prototype._blobPositionInFile = function (index) {
  return (index - 1) % this._blobsPerFile + 1
}

prototype._firstBlobInFile = function (index) {
  return this._blobPositionInFile(index) === 1
}

// Public API

prototype.directory = function () {
  return this._directory
}

prototype.files = function () {
  var returned = []
  for (var counter = 1; counter <= this._tailFileNumber; counter++) {
    returned.push(this._fileNumberToPath(counter))
  }
  return returned
}

prototype.length = function () {
  return this._length
}

// Create and return a pass-through Transform stream piped to the
// internal buffering stream that will be piped to an BlobLogEncoder.
prototype.createWriteStream = function () {
  var returned = through2.obj()
  returned.pipe(this._writeBuffer, {end: false})
  return returned
}

// Create a `Readable` stream.  Pipe a succession of `Readable` file
// stream plus `BlobLogDecoder` pipelines to it.
prototype.createReadStream = function () {
  var self = this
  var fileNumber = 0
  return MultiStream.obj(function (callback) {
    fileNumber++
    if (fileNumber > self._tailFileNumber) {
      // No more `Readable` streams.
      callback(null, null)
    } else {
      var filePath = self._fileNumberToPath(fileNumber)
      var readStream = fs.createReadStream(filePath)
      var decoder = new BlobLogDecoder()
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

var INTEGER_ENCODING = 'hex'

function packInteger (integer) {
  return lexint.pack(integer, INTEGER_ENCODING)
}

function unpackInteger (string) {
  return lexint.unpack(string, INTEGER_ENCODING)
}
