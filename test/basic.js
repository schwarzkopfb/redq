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
    var err = new Error('fake')
    setTimeout(function () {
        if (Math.random() > .5)
            done(err)
        else
            done()
    }, 10)
})

queue.listen(function () {
    console.log('listening')
})

var i = 0
var interval = setInterval(function () {
    queue.push({ test: i++ })
}, 50)

process.on('SIGINT', function () {
    console.log()
    clearInterval(interval)
    queue.close(function () {
        console.log('closed')
    })
})
