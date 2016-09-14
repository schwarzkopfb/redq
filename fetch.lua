local key = KEYS[ 1 ]
local id  = redis.call('lpop', key)

if id then
    local res = redis.call('hgetall', key .. ":" .. id)
    table.insert(res, 1, id)
    return res
else
    return nil
end
