local key = KEYS[ 1 ] --key base
local ts  = KEYS[ 2 ] --timestamp
local id  = redis.call('lpop', key)

if id then
    redis.call('zadd', key .. ":active", ts, id)
    local res = redis.call('hgetall', key .. ":" .. id)
    table.insert(res, 1, id)
    return res
else
    return nil
end
