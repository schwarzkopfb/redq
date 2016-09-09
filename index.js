'use strict'

module.exports = RedisQueue

var assert       = require('assert'),
    inherits     = require('util').inherits,
    EventEmitter = require('events').EventEmitter,
    id           = require('mdbid'),
    once         = require('once'),
    redis        = require('redis')

var defaultOptions = {
    ttl: Infinity,
    prefix: 'rq:',
    timeout: Infinity,
    concurrency: 1
}

function RedisQueue(name, options) {
    if (!(this instanceof RedisQueue))
        return new RedisQueue(name, options)

    assert(name, 'queue name is required')
    assert.equal(typeof name, 'string', 'queue name cannot be empty')
    assertOptions(options)

    EventEmitter.call(this)

    if (options.client instanceof redis.RedisClient)
        var client = options.client
    else
        client = redis.createClient(options)

    this.pending = 0
    this.name    = name
    this.pub     = client
    this.sub     = client.duplicate()

    var opts = mergeOptions(options)
    this.ttl         = opts.ttl
    this.prefix      = opts.prefix
    this.timeout     = opts.timeout
    this.concurrency = opts.concurrency

    var onerror = this.onerror.bind(this),
        onend   = this.onend.bind(this)

    this.pub
        .on('error', onerror)
        .on('end', onend)
    this.sub
        .on('error', onerror)
        .on('end', onend)
}

inherits(RedisQueue, EventEmitter)

function assertOptions(opts) {
    if (!opts)
        return

    if ('ttl' in opts)
        assert(opts.ttl > 0, '`options.ttl` must be greater than zero')

    if ('prefix' in opts)
        assert.equal(typeof opts.prefix, 'string', '`options.prefix` must be a string')

    if ('timeout' in opts)
        assert(opts.timeout > 0, '`options.timeout` must be greater than zero')

    if ('concurrency' in opts)
        assert(opts.concurrency > 0, '`opts.concurrency` must be greater than zero')
}

function mergeOptions(opts) {
    var merged = {}

    Object.keys(defaultOptions).forEach(function (key) {
        merged[ key ] = opts[ key ] || defaultOptions[ key ]
    })

    return merged
}

RedisQueue.prototype.key = function createKey(postfix) {
    var key = this.prefix + this.name

    if (postfix)
        key += ':' + postfix

    return key
}

RedisQueue.prototype.listen = function listen(callback) {
    if (this.listening)
        return
    else if (this.closed)
        throw new Error('queue (' + this.name + ') already closed')

    var self = this

    if (callback)
        this.on('listening', callback)

    this.sub.on('message', this.process.bind(this))
    this.sub.subscribe(this.key(), function () {
        self.listening = true
        self.emit('listening')
        self.process()
    })
}

RedisQueue.prototype.push = function push(data, callback) {
    if (this.closing)
        return
    else if (this.closed)
        throw new Error('queue (' + this.name + ') already closed')

    var iid = id(),
        key = this.key()

    this.pub
        .multi()
        .hmset(this.key(iid), data)
        .rpush(key, iid)
        .publish(key, iid)
        .exec(function (err) {
            if (callback)
                callback(err, iid)
        })
}

RedisQueue.prototype.remove = function remove(id, callback) {
    if (this.closing)
        return
    else if (this.closed)
        throw new Error('queue (' + this.name + ') already closed')

    this.pub
        .multi()
        .lrem(this.key(), 0, id)
        .del(this.key(id))
        .exec(function (err) {
            if (callback)
                callback(err, id)
        })
}

RedisQueue.prototype.close = function close(callback) {
    if (this.closing)
        return
    else if (this.closed)
        throw new Error('queue (' + this.name + ') already closed')

    if (callback)
        this.on('close', callback)

    this.closing = true
    this.sub.unsubscribe(this.key())
    this.exit()
}

RedisQueue.prototype.end = function end(flush) {
    if (this.closing)
        return
    else if (this.closed)
        throw new Error('queue (' + this.name + ') already closed')

    assert(
        flush === true || flush === false,
        '`flush` parameter must be explicitly set'
    )

    this.closed = true
    this.pub.end(flush)
    this.sub.end(flush)
}

RedisQueue.prototype.process = function processItems() {
    if (!this.listening || this.closing || this.closed)
        return

    var i = this.concurrency - this.pending
    while (i-- > 0)
        this.fetch()
}

RedisQueue.prototype.fetch = function fetch() {
    var self = this

    this.pending++
    this.pub.lpop(this.key(), function (err, id) {
        if (err) {
            self.onerror(err)
            self.enqueue(id)
        }
        else if (id) {
            var key = self.key(id)

            self.pub.hgetall(key, function (err, data) {
                if (err) {
                    self.onerror(err)
                    self.enqueue(id)
                }
                else if (data) {
                    // todo: timeout
                    self.emit('item', id, data, once(function done(err) {
                        if (err)
                            self.enqueue(id)
                        else
                            self.pub.del(key, self.done.bind(self))
                    }))
                }
                /* note:
                 * since we're not fetching queue items atomically,
                 * - by design - it's possible that the item spcified
                 * by the current `id` got removed in the meantime, but
                 * then we can safely ignore it
                 */
                else
                    self.done()
            })
        }
        else
            self.done()
    })
}

RedisQueue.prototype.done = function done(err) {
    if (this.closed)
        return

    this.pending--

    if (this.closing)
        this.exit()
    else
        this.process()
}

RedisQueue.prototype.exit = function exit() {
    if (!this.pending) {
        this.pub.quit()
        this.sub.quit()
    }
}

RedisQueue.prototype.enqueue = function enqueue(id) {
    var key = this.key()

    this.pub
        .multi()
        .rpush(key, id)
        .publish(key, id)
        .exec(this.done.bind(this))
}

RedisQueue.prototype.onerror = function onerror(err) {
    this.emit('error', err)
}

RedisQueue.prototype.onend = function onerror(err) {
    if (this.closed)
        return

    if (!this.pub.connected && !this.sub.connected) {
        this.closed = true
        this.emit('close')
    }
}
