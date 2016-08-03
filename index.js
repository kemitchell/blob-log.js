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
  self._index = null

  EventEmitter.call(self)

  // Initialize.
  self._writeBuffer = through2.obj()

  runSeries([
    mkdirp.bind(null, self._directory),
    self._readExistingFiles.bind(self),
    self._checkTailFile.bind(self),
    self._setupWriteStream.bind(self)
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
        var missingPath = self._fileNumberToPath(missing - 1)
        callback(new Error('missing ' + missingPath))
      } else {
        // Store the number of the latest log file.
        self._tailFileNumber = fileNumbers[fileNumbers.length - 1]
        callback()
      }
    }
  })
}

// Read any existing tail log file to determine what index numbers it
// contains and how much room for more blobs is left in it.
prototype._checkTailFile = function (callback) {
  var self = this
  // No existing tail log file.
  if (self._tailFileNumber === undefined) {
    self._blobsInTailFile = 0
    self._index = 0
    callback()
  // Have a tail log file.
  } else {
    var lastIndex = 0
    var blobCount = 0
    fs.createReadStream(self._tailFilePath())
    .once('error', /* istanbul ignore next */ function (error) {
      if (error.code === 'ENOENT') {
        self._blobsInTailFile = 0
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
        self._blobsInTailFile = blobCount
        self._index = lastIndex
        callback()
      })
    )
  }
}

// Create a Writable stream that encodes blobs and writes them to
// successively numbered log files.  Then hook the buffering stream
// created before we started checking for existing log files in the
// directory up to that Writable with a pipe.
prototype._setupWriteStream = function (callback) {
  var self = this
  // Generate a succession of Encoder Transform streams piped to file
  // write streams, ensuring:
  //
  // 1. `self._blobsPerFile` blobs are written to each file
  //
  // 2. Log files are named numerically, with packed integer basenames
  //    and `LOG_FILE_EXTENSION` extensions.
  //
  // 3. If there are existing log files, append to, rather than
  //    overwrite the current tail file.
  function sinkFactory (currentSink, chunk, encoding, callback) {
    var index = self._index + 1
    self._index = index
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
    // 1. This BlobLog was just initialized.  If there is room at the
    //    end of the existing log tail file, we should append there.
    //    Otherwise, we should start a new file.
    //
    // 2. This blob belongs in a new file we should create.
    } else {
      // If we are appending to an existing file, do not write a first
      // sequence number.
      var encoder = new Encoder(firstBlobInFile ? index : undefined)
      encoder.path = filePath
      var writeStream = fs.createWriteStream(filePath, {
        flags: firstBlobInFile ? 'w' : 'a'
      })
      if (firstBlobInFile) {
        self._blobsInTailFile = 1
      } else {
        self._blobsInTailFile++
      }
      pump(encoder, writeStream)
      callback(null, encoder)
    }
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

// Give a file path like `01.bloblog`, returns `Number(1)`.
// Given a file path like `other-file.txt` returns `undefined`.
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

prototype._nthInFile = function (index) {
  return (index - 1) % this._blobsPerFile + 1
}

prototype._firstBlobInFile = function (index) {
  return this._nthInFile(index) === 1
}

// Public API

prototype.getDirectory = function () {
  return this._directory
}

prototype.length = function () {
  return this._index
}

prototype.createWriteStream = function () {
  var returned = through2.obj()
  returned.pipe(this._writeBuffer, {end: false})
  return returned
}

prototype.createReadStream = function () {
  var self = this
  var fileNumber = 0
  return MultiStream.obj(function (callback) {
    fileNumber++
    if (fileNumber > self._tailFileNumber) {
      callback(null, null)
    } else {
      var filePath = self._fileNumberToPath(fileNumber)
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
