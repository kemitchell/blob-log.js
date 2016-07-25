var BlobLog = require('./')
var path = require('path')
var crypto = require('crypto')
var fs = require('fs')
var lexint = require('lexicographic-integer')
var tape = require('tape')
var temporaryDirectory = require('temporary-directory')

tape('first', function (test) {
  temporaryDirectory(function (error, directory, cleanup) {
    test.ifError(error)
    var log = new BlobLog({
      blobsPerFile: 10,
      checkBytes: 256,
      checkFunction: sha256,
      directory: directory
    })
    log.initialize(function (error) {
      test.ifError(error)
      var entry = new Buffer('this is a test', 'utf8')
      log.append(entry, function (error) {
        test.ifError(error)
        var stats = fs.statSync(
          path.join(directory, lexint.pack(0, 'hex'))
        )
        test.equal(stats.isFile(), true)
        cleanup()
        test.end()
      })
    })
  })
})

function sha256 (argument) {
  return crypto.createHash('sha256')
  .update(argument)
  .digest()
}
