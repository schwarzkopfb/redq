'use strict'

var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD
}

var Queue = require('../'),
    queue = new Queue('test', opts)

queue.on('item', function (id, data, done) {
    console.log('item:', id, data)
    done()
})

queue.listen()

var i = 0
var interval = setInterval(function () {
    queue.push({ test: i++ }, function (err, id) {
        if (err)
            throw err
        else
            queue.remove(id, function (err) {
                if (err)
                    throw err
                else
                    console.log('added and removed:', id)
            })
    })
}, 100)

process.on('SIGINT', function () {
    console.log()
    clearInterval(interval)
    queue.close(function () {
        console.log('closed')
    })
})
