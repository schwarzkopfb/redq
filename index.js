'use strict'

module.exports = RedisQueue

var fs           = require('fs'),
    join         = require('path').join,
    assert       = require('assert'),
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

function parseReply(arr) {
    var id  = arr.shift(),
        len = arr.length,
        res = {}

    for (var i = 0; i < len; i += 2)
        res[ arr[ i ] ] = arr[ i + 1 ]

    return [ id, res ]
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

    this.sub.on('message', this._process.bind(this))
    this.sub.subscribe(this.key(), function () {
        self.listening = true
        self.emit('listening')
        self._process()
    })
}

RedisQueue.prototype.add = function add(data, callback) {
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
    this._exit()
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

    this.closed    = true
    this.closing   = false
    this.listening = false
    this.pub.end(flush)
    this.sub.end(flush)
}

RedisQueue.prototype.enqueue = function enqueue(id) {
    var key = this.key()

    this.pub
        .multi()
        .rpush(key, id)
        .publish(key, id)
        .exec(this.ondone.bind(this))
}

RedisQueue.prototype._process = function processItems() {
    if (!this.listening || this.closing || this.closed)
        return

    var i = this.concurrency - this.pending
    while (i-- > 0)
        this._fetch()
}

RedisQueue.prototype._getFetchScriptSha = function getFetchScriptSha(cb) {
    var sha = this._fetchScriptSha

    if (sha) {
        cb(null, sha)
        return
    }

    var self   = this,
        client = this.pub

    fs.readFile(join(__dirname, 'fetch.lua'), { encoding: 'utf8' }, function (err, src) {
        if (err)
            cb(err)
        else
            client.script('load', src, function (err, sha) {
                if (err)
                    cb(err)
                else {
                    self._fetchScriptSha = sha
                    cb(null, sha)
                }
            })
    })
}

RedisQueue.prototype._fetch = function fetch() {
    var self = this

    this.pending++
    this._getFetchScriptSha(function (err, sha) {
        if (err)
            self.onerror(err)
        else {
            var key = self.key()

            self.pub.evalsha(sha, 1, key, function (err, arr) {
                if (err)
                    self.onerror(err)
                else if (arr) {
                    arr = parseReply(arr)

                    var id   = arr[ 0 ],
                        data = arr[ 1 ]

                    self.emit('item', id, data, once(function done(err) {
                        if (err)
                            self.enqueue(id)
                        else
                            self.pub.del(key + ':' + id, self.ondone.bind(self))
                    }))
                }
                else
                    self.pending--
            })
        }
    })
}

RedisQueue.prototype._exit = function exit() {
    if (!this.pending) {
        this.pub.quit()
        this.sub.quit()
    }
}

RedisQueue.prototype.ondone = function done(err) {
    if (this.closed)
        return

    this.pending--

    if (this.closing)
        this._exit()
    else
        this._process()
}

RedisQueue.prototype.onerror = function onerror(err) {
    this.emit('error', err)
}

RedisQueue.prototype.onend = function onerror(err) {
    if (this.closed)
        return

    if (!this.pub.connected && !this.sub.connected) {
        this.closed    = true
        this.closing   = false
        this.listening = false
        this.emit('close')
    }
}
