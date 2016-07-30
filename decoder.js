var Transform = require('readable-stream').Transform
var inherits = require('util').inherits
var through2 = require('through2')

module.exports = Decoder

function Decoder () {
  if (!(this instanceof Decoder)) {
    return new Decoder()
  }

  this._firstIndexOctets = []
  this._firstIndex = false
  this._blobsEmitted = 0

  Transform.call(this, {readableObjectMode: true})
}

inherits(Decoder, Transform)

var prototype = Decoder.prototype

prototype._resetCurrentBlob = function (index) {
  this._currentBlob = {
    index: this._firstIndex + this._blobsEmitted,
    length: false,
    lengthOctets: [],
    bytesToRead: false,
    crc: false,
    crcOctets: [],
    stream: through2()
  }
}

prototype._emit = function () {
  var blob = this._currentBlob
  this.push({
    index: blob.index,
    length: blob.length,
    crc: blob.crc,
    stream: blob.stream
  })
  this._blobsEmitted++
}

prototype._transform = function (chunk, encoding, callback) {
  var offset = 0
  var blob = this._currentBlob
  while (offset < chunk.length) {
    // Read the index number at the start of the file.
    if (!this._firstIndex) {
      var indexOctets = this._firstIndexOctets
      indexOctets.push(chunk[offset])
      if (indexOctets.length === 4) {
        this._firstIndex = new Buffer(indexOctets).readUInt32BE()
        delete this._indexOctets
      }
      offset++
    } else {
      // Create state for a new blob.
      if (blob === undefined || blob.bytesToRead === 0) {
        this._resetCurrentBlob()
        blob = this._currentBlob
      }
      // Read the length of this blob.
      if (!blob.length) {
        var lengthOctets = blob.lengthOctets
        lengthOctets.push(chunk[offset])
        if (lengthOctets.length === 4) {
          var length = new Buffer(lengthOctets).readUInt32BE()
          blob.length = length
          blob.bytesToRead = length
          delete blob.lengthOctets
        }
        offset++
      // Read the CRC.
      } else if (!blob.crc) {
        var crcOctets = blob.crcOctets
        crcOctets.push(chunk[offset])
        if (crcOctets.length === 4) {
          blob.crc = new Buffer(crcOctets).readUInt32BE()
          delete blob.crcOctets
          this._emit()
        }
        offset++
      // Read blob data.
      } else {
        var dataChunk = chunk.slice(offset, offset + blob.bytesToRead)
        var bytesSliced = dataChunk.length
        blob.bytesToRead -= bytesSliced
        blob.stream.write(dataChunk)
        if (blob.bytesToRead === 0) blob.stream.end()
        offset += bytesSliced
      }
    }
  }
  callback()
}
