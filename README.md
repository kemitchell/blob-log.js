```javascript
var BlobLog = require('blob-log')
var crypto = require('crypto')
var mapSeries = require('async.mapseries')
var assert = require('assert')
var randomString = require('random-string')

var EXAMPLE_HASHES = []
while (EXAMPLE_HASHES.length < 2000) {
  EXAMPLE_HASHES.push(
    crypto.createHash('sha256')
    .update(randomString(), 'utf8')
    .digest('hex')
  )
}

var log = new BlobLog({
  hashLength: 64,
  hashesPerFile: 1000
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
      assert.ifError(error, 'no error')
    })
    .once('end', function () {
      assert.deepEqual(
        hashes, EXAMPLE_HASHES.slice(3),
        'streams hashes'
      )
    })
  }
)
```
