```javascript
var BlobLog = require('blob-log')
var log = new BlobLog({
  directory: '/some/place/for/files',
  blobsPerFile: 100
})

log.directory() // => '/some/place/for/files'
log.files() // => Array of log file paths
log.length() // => number of log files

// Writable stream in object mode for blobs to store in the log.
log.createWriteStream()
// Objects must be Buffer objects.
.end(new Buffer('some content', 'ascii'))

// Readable stream of blobs stored in the log.
log.createReadStream()
.on('data', function (data) {
  data.index // Number
  data.length // Number, bytes
  data.crc // Number, CRC32
  data.stream // Normal-mode (binary) Readble stream
})

// Close streams.
log.close()
```
