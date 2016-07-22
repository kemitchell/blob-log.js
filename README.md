```javascript
var BlobLog = require('./')
var crypto = require('crypto')
var mapSeries = require('async.mapseries')
var rimraf = require('rimraf')
var assert = require('assert')

var EXAMPLE_ENTRIES = ['a', 'b', 'c', 'd', 'e', 'f']

var EXAMPLE_HASHES = EXAMPLE_ENTRIES.map(function (entry) {
  return crypto.createHash('sha256')
  .update(entry, 'utf8')
  .digest('hex')
})

var log = new BlobLog({
  hashLength: 64,
  hashesPerFile: 2,
})

mapSeries(
  EXAMPLE_HASHES,
  function (hash, done) {
    log.write(hash, done)
  },
  function (error) {
    assert.ifError(error)
    var hashes = []
    log.createReadStream(3)
    .on('data', function (chunk) {
      hashes.push(chunk)
    })
    .once('error', function (error) {
      assert.ifError(error)
    })
    .once('end', function () {
      assert.deepEqual(hashes, EXAMPLE_HASHES.slice(2), 'stream hashes')
    })
  }
)
```
