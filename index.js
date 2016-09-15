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
    prefix: 'rq:',
    concurrency: 1
}

function RedisQueue(name, options) {
    if (!(this instanceof RedisQueue))
        return new RedisQueue(name, options)

    assert(name, 'queue name is required')
    assert.equal(typeof name, 'string', 'queue name cannot be empty')
    assertOptions(options)

    EventEmitter.call(this)

    if (options && 'client' in options) {
        assert(
            options.client instanceof redis.RedisClient,
            'client must be a `RedisClient` instance'
        )
        var client = options.client
    }
    else
        client = redis.createClient(options)

    this.pending = 0
    this.name    = name
    this.pub     = client
    this.sub     = client.duplicate()

    var opts = mergeOptions(options)
    this.prefix      = opts.prefix
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

RedisQueue.Queue = RedisQueue

function assertOptions(opts) {
    if (!opts)
        return

    if ('prefix' in opts)
        assert.equal(typeof opts.prefix, 'string', '`options.prefix` must be a string')

    if ('concurrency' in opts)
        assert(opts.concurrency > 0, '`opts.concurrency` must be greater than zero')
}

function mergeOptions(opts) {
    if (!opts)
        opts = {}

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

function emit(queue, event, arg) {
    process.nextTick(function () {
        queue.emit(event, arg)
    })
}

RedisQueue.prototype.key = function createKey(postfix) {
    var key = this.prefix + this.name

    if (postfix)
        key += ':' + postfix

    return key
}

RedisQueue.prototype.listen = function listen(callback) {
    if (this.listening)
        return this

    assert(!this.closing && !this.closed, 'queue is already closed')

    var self = this

    if (callback)
        this.on('listening', callback)

    this.sub.on('message', this.process.bind(this))
    this.sub.subscribe(this.key(), function (err) {
        if (err) {
            self.onerror(err)
            return
        }

        self.listening = true
        emit(self, 'listening')
        self.process()
    })

    return this
}

RedisQueue.prototype.add = function add(data, callback) {
    assert(!this.closing && !this.closed, 'queue is already closed')

    var self = this,
        iid  = id(),
        key  = this.key()

    this.pub
        .multi()
        .hmset(this.key(iid), data)
        .rpush(key, iid)
        .publish(key, iid)
        .exec(function (err) {
            if (callback)
                callback(err, iid)
            else if (err)
                self.onerror(err)
        })

    return this
}

RedisQueue.prototype.remove = function remove(id, callback) {
    assert(!this.closing && !this.closed, 'queue is already closed')

    var self = this

    this.pub
        .multi()
        .lrem(this.key(), 0, id)
        .del(this.key(id))
        .exec(function (err) {
            if (callback)
                callback(err, id)
            else if (err)
                self.onerror(err)
        })

    return this
}

RedisQueue.prototype.close = function close(callback) {
    assert(!this.closing && !this.closed, 'queue is already closed')

    if (callback)
        this.on('close', callback)

    this.closing = true
    this.sub.unsubscribe(this.key())
    this._exit()
}

RedisQueue.prototype.enqueue = function enqueue(id) {
    assert(!this.closing && !this.closed, 'queue is already closed')

    var key = this.key()

    this.pub
        .multi()
        .rpush(key, id)
        .publish(key, id)
        .exec(this.ondone.bind(this))
}

RedisQueue.prototype.process = function processItems() {
    assert(!this.closing && !this.closed, 'queue is already closed')

    var i = this.concurrency - this.pending
    while (i-->0)
        this._fetch()
}

RedisQueue.prototype._getFetchScriptSha = function getFetchScriptSha(cb) {
    var sha = this._fetchScriptSha

    if (sha) {
        cb(sha)
        return
    }

    var self   = this,
        client = this.pub

    fs.readFile(join(__dirname, 'fetch.lua'), { encoding: 'utf8' }, function (err, src) {
        if (err)
            self.onerror(err)
        else
            client.script('load', src, function (err, sha) {
                if (err)
                    self.onerror(err)
                else {
                    self._fetchScriptSha = sha
                    cb(sha)
                }
            })
    })
}

RedisQueue.prototype._fetch = function fetch() {
    var self = this,
        key  = this.key()

    this.pending++
    this._getFetchScriptSha(function (sha) {
        self.pub.evalsha(sha, 2, key, Date.now(), function (err, arr) {
            if (err) {
                self.pending--
                self.onerror(err)
            }
            else if (arr) {
                arr = parseReply(arr)

                var id   = arr[ 0 ],
                    data = arr[ 1 ]

                self.emit('item', id, data, once(function done(err) {
                    if (err) {
                        self.pending--
                        self.enqueue(id)
                    }
                    else
                        self.pub
                            .multi()
                            .del(key + ':' + id)
                            .zrem(key + ':active', id)
                            .exec(self.ondone.bind(self))
                }))
            }
            else if (--self.pending === 0) {
                self.emit('idle')

                if (self.closing)
                    self._exit()
            }
        })
    })
}

RedisQueue.prototype._exit = function exit() {
    if (!this.pending) {
        this.pub.quit()
        this.sub.quit()
    }
}

RedisQueue.prototype.ondone = function ondone(err) {
    if (err) {
        self.onerror(err)
        return
    }

    this.pending--

    if (this.closing)
        this._exit()
    else
        this.process()
}

RedisQueue.prototype.onerror = function onerror(err) {
    emit(this, 'error', err)
}

RedisQueue.prototype.onend = function onend(err) {
    if (err) {
        self.onerror(err)
        return
    }

    if (!this.pub.connected && !this.sub.connected) {
        this.closed    = true
        this.closing   = false
        this.listening = false
        emit(this, 'close')
    }
}
