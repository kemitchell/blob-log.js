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

// Readable stream of blobs stored in the log.
log.createReadStream()

// Close streams.
log.close()
```
