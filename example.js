var smalltable = require('./')

var db = smalltable('./db', {
  keyLength: 5,
  valueLength: 5
})

db.put('hello', 'world', function (err) {
  if (err) throw err
  db.get('hello', function (err, value) {
    if (err) throw err
    console.log('hello -> ' + value)
  })
})

// db.list().on('data', function (data) {
//   console.log(data.value.toString())
// })
