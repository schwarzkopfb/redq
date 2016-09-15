'use strict'

if (module === require.main)
    return require('tap').pass()

var assert = require('assert'),
    env    = process.env

var msg = 'database number must be explicitly specified'
assert.notStrictEqual(env.REDIS_DATABASE, undefined, msg)
assert.notStrictEqual(env.REDIS_DATABASE, '', msg)
assert(!isNaN(env.REDIS_DATABASE), msg)

module.exports = {
    db:       env.REDIS_DATABASE,
    path:     env.REDIS_PATH,
    host:     env.REDIS_HOST,
    port:     env.REDIS_PORT,
    password: env.REDIS_PASSWORD
}
