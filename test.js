var BlobLog = require('./')
var crypto = require('crypto')
var mapSeries = require('async.mapseries')
var rimraf = require('rimraf')
var tape = require('tape')
var uuid = require('uuid').v4

var EXAMPLE_ENTRIES = ['a', 'b', 'c', 'd', 'e', 'f']

var EXAMPLE_HASHES = EXAMPLE_ENTRIES.map(function (entry) {
  return crypto.createHash('sha256')
  .update(entry, 'utf8')
  .digest('hex')
})

tape(function (test) {
  var directory = uuid()
  var log = new BlobLog({
    hashLength: 64,
    hashesPerFile: 2,
    directory: directory
  })
  mapSeries(
    EXAMPLE_HASHES,
    function (hash, done) {
      log.write(hash, done)
    },
    function (error) {
      test.ifError(error)
      var hashes = []
      log.createReadStream()
      .on('data', function (chunk) {
        hashes.push(chunk)
      })
      .once('error', function (error) {
        test.ifError(error, 'no error')
      })
      .once('end', function () {
        test.deepEqual(hashes, EXAMPLE_HASHES, 'stream hashes')
        rimraf(directory, function () {
          test.end()
        })
      })
    }
  )
})

tape(function (test) {
  var directory = uuid()
  var log = new BlobLog({
    hashLength: 64,
    hashesPerFile: 2,
    directory: directory
  })
  mapSeries(
    EXAMPLE_HASHES,
    function (hash, done) {
      log.write(hash, done)
    },
    function (error) {
      test.ifError(error)
      var hashes = []
      log.createReadStream(3)
      .on('data', function (chunk) {
        hashes.push(chunk)
      })
      .once('error', function (error) {
        test.ifError(error)
      })
      .once('end', function () {
        test.deepEqual(hashes, EXAMPLE_HASHES.slice(2), 'stream hashes')
        rimraf(directory, function () {
          test.end()
        })
      })
    }
  )
})
