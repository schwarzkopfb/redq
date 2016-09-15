'use strict'

var test  = require('tap'),
    queue = require('./queue')(),
    item  = { test: 42 },
    sent,
    onadd,
    onitem

test.plan(8)

queue.on('item', function (id, data, done) {
    if (onitem)
        test.fail('`onitem` fired more than once')
    else
        onitem = true

    test.equal(id, sent, 'same id should be received')
    test.same(data, item, 'same object should be received')
    test.type(done, 'function', 'a callback function should be received')
    done()
    queue.close(function () {
        test.pass('`close` event emitted')
    })
})

queue.add(item, function (err, id) {
    if (onadd)
        test.fail('`onadd` fired more than once')
    else
        onadd = true

    test.pass('`add()` callback fired')
    test.ok(/[0-9a-f]{24}/.test(id), 'id should be a 12 bytes length hexadecimal string')

    if (err)
        test.threw(err)
    else {
        sent = id
        queue.listen(function () {
            test.pass('`listening` event emitted')
            test.doesNotThrow(queue.listen.bind(queue), 're-invocation of `listen()` should not throw')
        })
    }
})
