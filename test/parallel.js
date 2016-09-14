'use strict'

var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
    concurrency: 5
}

var Queue = require('../'),
    queue1 = new Queue('test', opts),
    queue2 = new Queue('test', opts)

var ids    = [],
    counts = []

function onitem(id, data, done) {
    ids.push(id)
    counts.push(data.i)
    done()
}

queue1.on('item', onitem).listen()
queue2.on('item', onitem).listen()

console.time('elapsed')
var i = 0
var interval = setInterval(function () {
    queue1.add({ i: i++ })
    queue2.add({ i: i++ })
}, 1)

process.on('SIGINT', function () {
    clearInterval(interval)
    queue1.close(onclose)
    queue2.close(onclose)
})

function isUnique(arr) {
    var all = {}

    for (var i = 0, l = arr.length; i < l; i++) {
        var item = arr[ i ]

        if (item in all)
            return false
        else
            all[ item ] = true
    }

    return true
}

var closed
function onclose() {
    if (closed) {
        console.log('ids are unique:', isUnique(ids))
        console.log('counts are unique:', isUnique(counts))
        console.log('processed:', ids.length)
        console.timeEnd('elapsed')
    }
    else
        closed = true
}
