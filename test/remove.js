'use strict'

var test  = require('tap'),
    queue = require('./queue')()

test.plan(1)

function handleError(fn) {
    return function (err) {
        if (err) {
            test.threw(err)

            if (!queue.closed)
                queue.close()
        }
        else
            fn.apply(null, Array.prototype.slice.call(arguments, 1))
    }
}

queue.on('item', function (id, data, done) {
    test.equal(data.n, '2', 'first item should be removed')
    done()
    queue.close()
})

queue.add({ n: 1 }, handleError(function (id) {
    queue.add({ n: 2 }, handleError(function () {
        queue.remove(id, handleError(function () {
            queue.listen()
        }))
    }))
}))
