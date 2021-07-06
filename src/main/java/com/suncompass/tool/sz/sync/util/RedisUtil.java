package com.suncompass.tool.sz.sync.util;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class RedisUtil {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public RedisUtil() {
    }

    public boolean expire(String key, long time) {
        try {

            if (time > 0L) {
                this.redisTemplate.expire(key, time, TimeUnit.SECONDS);
            }

            return true;

        } catch (Exception var5) {
            var5.printStackTrace();
            return false;
        }
    }

    public long getExpire(String key) {
        return this.redisTemplate.getExpire(key, TimeUnit.SECONDS);
    }

    public boolean hasKey(String key) {
        try {

            return this.redisTemplate.hasKey(key);

        } catch (Exception var3) {
            var3.printStackTrace();
            return false;
        }
    }

    public void del(String... key) {
        if (key != null && key.length > 0) {
            if (key.length == 1) {
                this.redisTemplate.delete(key[0]);
            } else {
                this.redisTemplate.delete(CollectionUtils.arrayToList(key));
            }
        }
    }

    public Object get(String key) {
        return key == null ? null : this.redisTemplate.opsForValue().get(key);
    }

    public boolean set(String key, Object value) {
        try {

            this.redisTemplate.opsForValue().set(key, value);
            return true;

        } catch (Exception var4) {
            var4.printStackTrace();
            return false;
        }
    }

    public boolean set(String key, Object value, long time) {
        try {

            if (time > 0L) {
                this.redisTemplate.opsForValue().set(key, value, time, TimeUnit.SECONDS);
            } else {
                this.set(key, value);
            }

            return true;

        } catch (Exception var6) {
            var6.printStackTrace();
            return false;
        }
    }

    public long incr(String key, long delta) {
        if (delta < 0L) {
            throw new RuntimeException("递增因子必须大于0");
        } else {
            return this.redisTemplate.opsForValue().increment(key, delta);
        }
    }

    public long decr(String key, long delta) {
        if (delta < 0L) {
            throw new RuntimeException("递减因子必须大于0");
        } else {
            return this.redisTemplate.opsForValue().increment(key, -delta);
        }
    }

    public Object hget(String key, String item) {
        return this.redisTemplate.opsForHash().get(key, item);
    }

    public Map<Object, Object> hmget(String key) {
        return this.redisTemplate.opsForHash().entries(key);
    }

    public boolean hmset(String key, Map<String, Object> map) {
        try {

            this.redisTemplate.opsForHash().putAll(key, map);
            return true;

        } catch (Exception var4) {
            var4.printStackTrace();
            return false;
        }
    }

    public boolean hmset(String key, Map<String, Object> map, long time) {
        try {

            this.redisTemplate.opsForHash().putAll(key, map);
            if (time > 0L) {
                this.expire(key, time);
            }

            return true;

        } catch (Exception var6) {
            var6.printStackTrace();
            return false;
        }
    }

    public boolean hset(String key, String item, Object value) {
        try {

            this.redisTemplate.opsForHash().put(key, item, value);
            return true;

        } catch (Exception var5) {
            var5.printStackTrace();
            return false;
        }
    }

    public boolean hset(String key, String item, Object value, long time) {
        try {

            this.redisTemplate.opsForHash().put(key, item, value);
            if (time > 0L) {
                this.expire(key, time);
            }

            return true;

        } catch (Exception var7) {
            var7.printStackTrace();
            return false;
        }
    }

    public void hdel(String key, Object... item) {
        this.redisTemplate.opsForHash().delete(key, item);
    }

    public boolean hHasKey(String key, String item) {
        return this.redisTemplate.opsForHash().hasKey(key, item);
    }

    public double hincr(String key, String item, double by) {
        return this.redisTemplate.opsForHash().increment(key, item, by);
    }

    public double hdecr(String key, String item, double by) {
        return this.redisTemplate.opsForHash().increment(key, item, -by);
    }

    public Set<Object> sGet(String key) {
        try {

            return this.redisTemplate.opsForSet().members(key);

        } catch (Exception var3) {
            var3.printStackTrace();
            return null;
        }
    }

    public boolean sHasKey(String key, Object value) {
        try {

            return this.redisTemplate.opsForSet().isMember(key, value);

        } catch (Exception var4) {
            var4.printStackTrace();
            return false;
        }
    }

    public long sSet(String key, Object... values) {
        try {

            return this.redisTemplate.opsForSet().add(key, values);

        } catch (Exception var4) {
            var4.printStackTrace();
            return 0L;
        }
    }

    public long sSetAndTime(String key, long time, Object... values) {
        try {

            Long count = this.redisTemplate.opsForSet().add(key, values);
            if (time > 0L) {
                this.expire(key, time);
            }

            return count;

        } catch (Exception var6) {
            var6.printStackTrace();
            return 0L;
        }
    }

    public long sGetSetSize(String key) {
        try {

            return this.redisTemplate.opsForSet().size(key);

        } catch (Exception var3) {
            var3.printStackTrace();
            return 0L;
        }
    }

    public long setRemove(String key, Object... values) {
        try {

            Long count = this.redisTemplate.opsForSet().remove(key, values);
            return count;

        } catch (Exception var4) {
            var4.printStackTrace();
            return 0L;
        }
    }

    public List<Object> lRange(String key, long start, long end) {
        try {

            return this.redisTemplate.opsForList().range(key, start, end);

        } catch (Exception var7) {
            var7.printStackTrace();
            return null;
        }
    }

    public long lSize(String key) {
        try {

            return this.redisTemplate.opsForList().size(key);

        } catch (Exception var3) {
            var3.printStackTrace();
            return 0L;
        }
    }

    public Object lIndex(String key, long index) {
        try {

            return this.redisTemplate.opsForList().index(key, index);

        } catch (Exception var5) {
            var5.printStackTrace();
            return null;
        }
    }

    public boolean lRightPush(String key, Object value) {
        try {

            this.redisTemplate.opsForList().rightPush(key, value);
            return true;

        } catch (Exception var4) {
            var4.printStackTrace();
            return false;
        }
    }

    public boolean lRightPush(String key, Object value, long time) {
        try {

            this.redisTemplate.opsForList().rightPush(key, value);
            if (time > 0L) {
                this.expire(key, time);
            }

            return true;

        } catch (Exception var6) {
            var6.printStackTrace();
            return false;
        }
    }

    public boolean lRightPushAll(String key, List<Object> value) {
        try {

            this.redisTemplate.opsForList().rightPushAll(key, value);
            return true;

        } catch (Exception var4) {
            var4.printStackTrace();
            return false;
        }
    }

    public boolean lRightPushAll(String key, List<Object> value, long time) {
        try {

            this.redisTemplate.opsForList().rightPushAll(key, value);
            if (time > 0L) {
                this.expire(key, time);
            }

            return true;

        } catch (Exception var6) {
            var6.printStackTrace();
            return false;
        }
    }

    public boolean lSet(String key, long index, Object value) {
        try {

            this.redisTemplate.opsForList().set(key, index, value);
            return true;

        } catch (Exception var6) {
            var6.printStackTrace();
            return false;
        }
    }

    public long lRemove(String key, long count, Object value) {
        try {

            Long remove = this.redisTemplate.opsForList().remove(key, count, value);
            return remove;

        } catch (Exception var6) {
            var6.printStackTrace();
            return 0L;
        }
    }
}