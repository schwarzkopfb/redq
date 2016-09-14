'use strict'

var opts = {
    db: process.env.REDIS_DB || 11,
    path: process.env.REDIS_PATH,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
    concurrency: 5
}

var Queue = require('../'),
    queue = new Queue('test', opts),
    count = 0,
    start = +new Date

function onclose() {
    var end     = +new Date,
        elapsed = ((end - start) / 1000).toFixed(2)

    console.log('processed:', count)
    console.log('elapsed:', elapsed)
    console.log('result:', (count / elapsed).toFixed(2), 'op/s')
}

queue.on('item', function (id, data, done) {
    count++
    done()
})

queue.on('idle', function () {
    console.log('[finished]')
    queue.close(onclose)
})

queue.listen()

process.on('SIGINT', function () {
    process.stdout.cursorTo(0)
    console.log('[terminated]')
    queue.close(onclose)
})
