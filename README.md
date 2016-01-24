# smalltable

An on-disk key-value store with very few features

```
npm install smalltable
```

## Usage

``` js
var smalltable = require('smalltable')
var db = smalltable('my.db', {
  hash: 'sha1', // use sha1 to hash the keys
  valueLength: 5 // values take up 5 bytes
})

db.put('hello', 'world', function (err) {
  if (err) throw err
  db.get('hello', function (err, value) {
    if (err) throw err
    console.log('hello -> ' + value)
  })
})
```

## API

#### `db = smalltable(filename, options)`

Create a new db instance. The data will stored in a file called `filename`.

Options include:

``` js
{
  hash: 'sha1',    // optional key hashing algorithm
  keyLength: 5,    // optional key byte length
  valueLength: 5,  // value byte length
  values: 128,     // optional inital value count
  truncate: false  // optional truncate the database
}
```

If you don't specify `hash` you have to specify `keyLength`. If you store more than `values` values the database will be automatically expanded to contain more values. Currently this requires re-indexing all values stored in the database.

#### `db.put(key, value, [cb])`

Store a value. If the value if less than `valueLength` it will be zero-padded.

#### `db.get(key, cb)`

Retrive a value. Callback is called with `cb(err, buffer)`. If the key isn't found an error is returned.

#### `db.del(key, [cb])`

Delete a value

#### `db.flush(cb)`

Flushes the database by performing an fsync on the underlying file descriptor

#### `db.close(cb)`

Closes the underlying file descriptor.

#### `stream = db.list()`

Get a stream of all values `{key: key, value: value}` in the database. If `hash` was set in the constructor the key will be the hash of the key.

## License

MIT
