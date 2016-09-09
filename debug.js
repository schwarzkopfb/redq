'use strict'

var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD
}

var redis  = require('redis'),
    client = redis.createClient(opts),
    argv   = process.argv

if (~argv.indexOf('--del'))
    client.keys('*', (err, keys) => {
        if (keys.length)
            client.del(keys, done)
        else
            done()
    })
else if (~argv.indexOf('--list'))
    client.lrange('rq:test', 0, -1, done)
else if (~argv.indexOf('--count'))
    client.llen('rq:test', done)
else
    client.keys('*', done)

function done(err, res) {
    if (err)
        console.error(err.stack)
    else if (res)
        console.log(res)

    client.unref()
}
