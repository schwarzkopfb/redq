/* expose a fresh test queue instance for further testing */

'use strict'

if (module === require.main)
    return require('tap').pass()

var cred  = require('./credentials'),
    name  = require('mdbid'),
    Queue = require('../')

module.exports = function () {
    return Queue(name(), cred)
}
