'use strict'

var EE    = require('events').EventEmitter,
    AE    = require('assert').AssertionError,
    redis = require('redis'),
    test  = require('tap'),
    cred  = require('./credentials'),
    name  = require('mdbid')(),
    Queue = require('../')

function onerror() {
    // this is a formal test, so ignore errors
}

test.type(Queue, 'function', 'main export should be a function')
test.equal(Queue, Queue.Queue, '`Queue` reference should be exposed')

function close(queue) {
    queue.on('error', onerror)
         .close()

    return queue
}

test.throws(
    function () {
        new Queue
    },
    AE, 'name should be required'
)
test.doesNotThrow(
    function () {
        close(new Queue(name))
    },
    'constructor should be used with `new`'
)
test.doesNotThrow(
    function () {
        close(Queue(name))
    },
    'constructor should be used without `new`'
)
test.doesNotThrow(
    function () {
        close(Queue(name), {})
    },
    'constructor should accept an options object'
)
test.throws(
    function () {
        close(new Queue(name, { prefix: null }))
    },
    AE, 'prefix option should be asserted'
)
test.doesNotThrow(
    function () {
        close(new Queue(name, { prefix: '' }))
    },
    'valid prefix option should be accepted'
)
test.throws(
    function () {
        close(new Queue(name, { concurrency: null }))
    },
    AE, 'concurrency option should be asserted'
)
test.throws(
    function () {
        close(new Queue(name, { concurrency: -1 }))
    },
    AE, 'concurrency option should be asserted'
)
test.throws(
    function () {
        close(new Queue(name, { concurrency: 0 }))
    },
    AE, 'concurrency option should be asserted'
)
test.doesNotThrow(
    function () {
        close(new Queue(name, { concurrency: 10 }))
    },
    'valid concurrency option should be accepted'
)
test.throws(
    function () {
        close(new Queue(name, { client: null }))
    },
    AE, 'client option should be asserted'
)
test.throws(
    function () {
        close(new Queue(name, { client: true }))
    },
    AE, 'client option should be asserted'
)
test.throws(
    function () {
        close(new Queue(name, { client: {} }))
    },
    AE, 'client option should be asserted'
)
test.doesNotThrow(
    function () {
        close(new Queue(name, { client: redis.createClient() }))
    },
    'valid client option should be accepted'
)

var q = close(new Queue(name))
test.ok(q instanceof EE, 'queue instance should be an EventEmitter')
test.equal(q.name, name, 'name should be stored on instance')
test.equal(q.prefix, 'rq:', 'prefix should have a default value')
test.equal(q.concurrency, 1, 'concurrency should have a default value')
test.ok(
    q.pub instanceof redis.RedisClient,
    'client should be created if not supplied to the constructor'
)
test.ok(
    q.sub instanceof redis.RedisClient,
    'client should be created if not supplied to the constructor'
)
test.type(q.add, 'function', '`add()` method should exist')
test.type(q.remove, 'function', '`remove()` method should exist')
test.type(q.listen, 'function', '`listen()` method should exist')
test.type(q.close, 'function', '`close()` method should exist')
test.type(q.enqueue, 'function', '`enqueue()` method should exist')
test.type(q.process, 'function', '`process()` method should exist')

test.throws(
    function () {
        q.listen()
    },
    AE, '`listen()` should throw if queue is already closed'
)
test.throws(
    function () {
        q.add()
    },
    AE, '`add()` should throw if queue is already closed'
)
test.throws(
    function () {
        q.remove()
    },
    AE, '`remove()` should throw if queue is already closed'
)
test.throws(
    function () {
        q.close()
    },
    AE, '`close()` should throw if queue is already closed'
)
test.throws(
    function () {
        q.enqueue()
    },
    AE, '`enqueue()` should throw if queue is already closed'
)
test.throws(
    function () {
        q.process()
    },
    AE, '`process()` should throw if queue is already closed'
)
