```javascript
var BlobLog = require('blob-log')
var assert = require('assert')
var crypto = require('crypto')
var fs = require('fs')
var mapSeries = require('async.mapseries')
var randomString = require('random-string')

// Generate some data entries and their hashes.
var EXAMPLE_HASHES = []
while (EXAMPLE_HASHES.length < 2000) {
  EXAMPLE_HASHES.push(
    crypto.createHash('sha256')
    .update(randomString())
    .digest('hex')
  )
}

// Create a new disk-persisted, append-only log.
var log = new BlobLog({
  // Hex-encoded SHA-256 hashes are 64 bytes.
  hashLength: 64,
  // Store 1000 SHA-256 hashes per file.
  // You may want to set this far higher.
  hashesPerFile: 1000,
  // Store log files in this directory.
  directory: '.blob-log'
})

mapSeries(
  // Append all of our entry hashes to the log.
  EXAMPLE_HASHES,
  function (hash, done) {
    log.write(hash, done)
  },
  // When that's done...
  function (error) {
    assert.ifError(error)
    // Stream and buffer the log's entries, from index 3.
    var hashes = []
    log.createReadStream(3)
    .on('data', function (chunk) {
      hashes.push(chunk)
    })
    .once('error', function (error) {
      assert.ifError(error, 'no error')
    })
    .once('end', function () {
      // Check that we received all entry hashes from index 3.
      assert.deepEqual(
        hashes, EXAMPLE_HASHES.slice(3),
        'streams hashes from start index'
      )
      // Check that all 2000 hashes were stored in two files of 1000
      // hashes each in the `.blob-log` directory.
      fs.stat('.blob-log/00', function (error, stat) {
        assert.ifError(error, 'no error')
        assert.equal(stat.isFile(), true)
        assert.equal(stat.size, 1000 * 64)
      })
      fs.stat('.blob-log/01', function (error, stat) {
        assert.ifError(error, 'no error')
        assert.equal(stat.isFile(), true)
        assert.equal(stat.size, 1000 * 64)
      })
    })
  }
)
```
