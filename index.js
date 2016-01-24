var open = require('random-access-open')
var crypto = require('crypto')
var equals = require('buffer-equals')
var stream = require('readable-stream')
var thunky = require('thunky')
var fs = require('fs')
var util = require('util')

var MAX_PROBES = 64

module.exports = SmallTable

function SmallTable (filename, opts) {
  if (!(this instanceof SmallTable)) return new SmallTable(filename, opts)
  var self = this

  if (!opts) opts = {}
  if (!opts.keyLength && !opts.hash) throw new Error('opts.keyLength or opts.hash is required')
  if (!opts.valueLength) throw new Error('opts.valueLength is required')

  this.filename = filename
  this.hash = opts.hash
  this.keyLength = opts.keyLength || crypto.createHash(this.hash).update('').digest().length
  this.valueLength = opts.valueLength
  this.chunkLength = this.valueLength + this.keyLength
  this.blankKey = blank(this.keyLength)
  this.blankValue = blank(this.valueLength)

  this.fd = 0
  this.size = 0
  this.values = 0
  this._wait = true

  this.open = thunky(ready)
  this.open()

  function ready (cb) {
    if (opts.truncate) fs.open(filename, 'w+', next)
    else open(filename, next)

    function next (err, fd) {
      if (err) return cb(err)
      fs.fstat(fd, function (err, st) {
        if (err) return onerror(fd, err)
        var size = st.size || ((opts.values || 64 * MAX_PROBES) * self.chunkLength)
        fs.ftruncate(fd, size, function (err) {
          if (err) return onerror(fd, err)
          self.size = size
          self.fd = fd
          self.values = Math.floor(self.size / self.chunkLength)
          self._wait = false
          cb()
        })
      })
    }

    function onerror (fd, err) {
      fs.close(fd, function () {
        cb(err)
      })
    }
  }
}

SmallTable.prototype.del = function (key, cb) {
  if (!cb) cb = noop
  if (typeof key === 'string') key = Buffer(key)
  if (this.hash) key = crypto.createHash(this.hash).update(key).digest()

  if (this._wait) this._defer(key, this.blankValue, cb)
  else this._visit(key, this.blankValue, cb)
}

SmallTable.prototype.put = function (key, value, cb) {
  if (!cb) cb = noop
  if (typeof key === 'string') key = Buffer(key)
  if (typeof value === 'string') value = Buffer(value)
  if (this.hash) key = crypto.createHash(this.hash).update(key).digest()

  if (this._wait) this._defer(key, value, cb)
  else this._visit(key, value, cb)
}

SmallTable.prototype.get = function (key, cb) {
  if (typeof key === 'string') key = Buffer(key)
  if (this.hash) key = crypto.createHash(this.hash).update(key).digest()

  if (this._wait) this._defer(key, null, cb)
  else this._visit(key, null, cb)
}

SmallTable.prototype.flush = function (cb) {
  if (!this.fd) return this.open(this.flush.bind(this, cb))
  fs.fsync(this.fd, cb || noop)
}

SmallTable.prototype.list = function () {
  return new ReadStream(this)
}

SmallTable.prototype._expand = function (cb) {
  if (!cb) cb = noop

  var self = this
  var buf = Buffer(65536 - (65536 % this.chunkLength))
  var pos = 0
  var missing = 0
  var expanded = new SmallTable(this.filename + '.expand', {
    valueLength: this.valueLength,
    keyLength: this.keyLength,
    values: this.values * 2,
    truncate: true
  })

  this._wait = true
  this.open = thunky(grow)
  this.open(cb)

  function grow (cb) {
    var error = null

    expanded.open(function (err) {
      if (err) return cb(err)
      fs.read(self.fd, buf, 0, buf.length, 0, onread)
    })

    function done () {
      fs.rename(expanded.filename, self.filename, function (err) {
        if (err) return cb(err)
        var oldFd = self.fd
        self.fd = expanded.fd
        fs.close(oldFd, function (err) {
          if (err) return cb(err)
          self.size = expanded.size
          self.values = expanded.values
          cb()
        })
      })
    }

    function check (err) {
      if (err) error = err
      if (--missing) return
      if (error) return cb(error)
      fs.read(self.fd, buf, 0, buf.length, pos, onread)
    }

    function onread (err, bytes) {
      if (err) return cb(err)
      if (!bytes) return done()

      for (var i = 0; i < bytes; i += self.chunkLength) {
        if (i + self.chunkLength > bytes) break
        pos += self.chunkLength
        var key = buf.slice(i, i + self.keyLength)
        if (!equals(key, self.blankKey)) {
          missing++
          var val = buf.slice(i + self.keyLength, i + self.keyLength + self.valueLength)
          expanded._visit(key, val, check)
        }
      }

      if (!missing) {
        missing = 1
        check()
      }
    }
  }
}

SmallTable.prototype._defer = function (key, value, cb) {
  var self = this
  this.open(function (err) {
    if (err) return cb(err)
    self._visit(key, value, cb)
  })
}

SmallTable.prototype._visit = function (hash, value, cb) {
  var pos = toNumber(hash) % this.values

  var writing = !!value
  var self = this
  var buf = Buffer(this.chunkLength)
  var probes = MAX_PROBES
  var flushing = false

  fs.read(this.fd, buf, 0, buf.length, this.chunkLength * pos, check)

  function check (err, bytes) {
    if (err) return cb(err)
    if (flushing) return cb(null)
    if (!bytes) buf.fill(0)

    var oldKey = buf.slice(0, hash.length)

    if (writing) {
      if (equals(self.blankKey, oldKey) || equals(hash, oldKey)) {
        if (value === self.blankValue) self.blankKey.copy(buf)
        else hash.copy(buf)
        value.copy(buf, hash.length)
        flushing = true
        fs.write(self.fd, buf, 0, buf.length, self.chunkLength * pos, check)
        return
      }
    } else {
      if (equals(hash, oldKey)) return cb(null, buf.slice(hash.length))
    }

    if (!probes) {
      if (!writing) return cb(new Error('Could not find key'))
      self._grow(hash, value, cb)
      return
    }

    if (pos === self.values - 1) pos = 0
    else pos++
    probes--

    fs.read(self.fd, buf, 0, buf.length, self.chunkLength * pos, check)
  }
}

SmallTable.prototype._grow = function (hash, value, cb) {
  var self = this
  this._expand(function (err) {
    if (err) return cb(err)
    self._visit(hash, value, cb)
  })
}

function ReadStream (table) {
  stream.Readable.call(this, {objectMode: true})

  var pos = 0
  var buf = Buffer(65536 - (65536 % table.chunkLength))

  this.destroyed = false
  this._table = table
  this._reading = false
  this._kick = kick

  var self = this

  function kick (err) {
    if (err) return self.destroy(err)
    fs.read(table.fd, buf, 0, buf.length, pos, onread)
  }

  function onread (err, bytes) {
    if (err) return self.destroy(err)
    if (!bytes) return self.push(null)

    var flushed = true
    for (var i = 0; i < bytes; i += table.chunkLength) {
      if (i + table.chunkLength > bytes) break
      pos += table.chunkLength

      var key = buf.slice(i, i + table.keyLength)
      if (!equals(key, table.blankKey)) {
        var copyKey = Buffer(table.keyLength)
        key.copy(copyKey)
        var copyValue = Buffer(table.valueLength)
        buf.copy(copyValue, 0, i + table.keyLength, i + table.keyLength + table.valueLength)
        flushed = self.push({key: copyKey, value: copyValue})
        if (self.destroyed) return
      }
    }
    self._reading = false
    if (flushed) self._read()
  }
}

util.inherits(ReadStream, stream.Readable)

ReadStream.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  if (err) this.emit('error', err)
  this.emit('close')
}

ReadStream.prototype._read = function () {
  if (this._reading || this.destroyed) return
  this._reading = true
  this._table.open(this._kick)
}

function toNumber (buf) {
  if (buf.length < 6) {
    switch (buf.length) {
      case 0: return 0
      case 1: return buf[0]
      case 2: return 256 * buf[0] + buf[1]
      case 3: return 65536 * buf.readUInt16BE(0) + buf[2]
      case 4: return buf.readUInt32BE(0)
      case 5: return 256 * buf.readUInt32BE(0) + buf[4]
    }
  }
  return 65536 * buf.readUInt32BE(0) + buf.readUInt16BE(4)
}

function noop () {}

function blank (n) {
  var buf = Buffer(n)
  buf.fill(0)
  return buf
}
