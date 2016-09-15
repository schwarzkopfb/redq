'use strict'

var test  = require('tap'),
    queue = require('./queue')(),
    count = 3,
    err   = new Error('test'),
    sent

test.plan(3)

queue.on('item', onitem)
     .on('idle', onidle)
     .add({ test: 42 }, onadd)

function onadd(err, id) {
    if (err)
        test.threw(err)
    else {
        sent = id
        queue.listen()
    }
}

function onitem(id, data, done) {
    if (count-->0) {
        test.equal(id, sent, 'item should be enqueued again')
        done(err)
    }
    else
        done()
}

function onidle() {
    queue.close()
}
