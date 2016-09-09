'use strict'

var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
    concurrency: 5
}

var Queue = require('../'),
    queue = new Queue('test', opts)

queue.on('item', function (id, data, done) {
    console.log('item:', id, data)
    setTimeout(function () {
        if (Math.random() > .5)
            done(new Error('fake'))
        else
            done()
    }, 10)
})

queue.listen()

var i = 0
var interval = setInterval(function () {
    queue.push({ test: i++ })
}, 50)

process.on('SIGINT', function () {
    clearInterval(interval)
    queue.close()
})
