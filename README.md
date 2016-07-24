```javascript
var BlobLog = require('blob-log')
var assert = require('assert')
var fs = require('fs')
var mapSeries = require('async.mapseries')

// Generate some data.
var EXAMPLE_BUFFERS = []
while (EXAMPLE_BUFFERS.length < 2000) {
  EXAMPLE_BUFFERS.push(
    Buffer.alloc(
      // Randomize size.
      random(8, 256),
      // Fill with a random capital letter.
      String.fromCharCode(random(65, 90))
    )
  )
}

function random (minimum, maximum) {
  return Math.floor(Math.random() * (maximum - minimum + 1)) + minimum
}

// Create a new disk-persisted, append-only log.
var log = new BlobLog({
  // Store 1000 bytes per file.
  hashesPerFile: 1000,
  // Store log files in this directory.
  directory: '.blob-log'
})

mapSeries(
  // Append all of our entry hashes to the log.
  EXAMPLE_BUFFERS,
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
        hashes, EXAMPLE_BUFFERS.slice(3),
        'streams hashes from start index'
      )
      return
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
