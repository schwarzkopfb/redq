var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD
}

var script = require('fs').readFileSync(__dirname + '/../fetch.lua'),
    client = require('redis').createClient(opts)

client.script('load', script, function (err, sha) {
    if (err)
        throw err

    client.evalsha(sha, 1, 'rq:test', function (err, data) {
        if (err)
            throw err

        if (data)
            console.log(parseReply(data))
        else
            console.log(data)
    })
})

function parseReply(reply) {
    var id  = reply.shift(),
        res = {}

    for (var i = 0, l = reply.length; i < l; i += 2)
        res[ reply[ i ] ] = reply[ i + 1 ]

    return [ id, res ]
}
