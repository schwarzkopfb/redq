'use strict'

var test  = require('tap'),
    queue = require('./queue')(),
    count = 3,
    ended = 0,
    err   = new Error('test'),
    sent

test.plan(3)

queue.on('item', onitem)
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
    ended++
    
    if (count-->0) {
        test.equal(id, sent, 'item should be enqueued again')
        done(err)
    }
    else if (ended < 3)
        done()
    else
        queue.close()
}
