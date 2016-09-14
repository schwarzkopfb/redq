var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
    concurrency: 5
}

var Queue = require('../'),
    queue = new Queue('test', opts),
    pending = 100000

for (var i = pending; i--;)
    queue.add({ test: 'test', other: 'test', 'another-test': 'test' }, done)

function done(err) {
    if (err)
        throw err

    --pending || queue.close()
}
