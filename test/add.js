var opts = {
    db: 11,
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT,
    password: process.env.REDIS_PASSWORD,
    concurrency: 5
}

var Queue = require('../'),
    queue = new Queue('test', opts)

for (var i = 10; i--;)
    queue.add({ test: 'test', other: 'test', 'another-test': 'test' })

queue.close()
