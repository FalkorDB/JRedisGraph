package com.redislabs.redisgraph.impl.api;

import redis.clients.jedis.Jedis;

/**
 * Pooled Connection implementation of the Connection interface for RedisGraph
 */
public class PooledConnection extends SingleConnection {

    public PooledConnection(Jedis jedis) {
        super(jedis);
    }

    @Override
    public void close() {
        // Doesn't actually close the connection, just returns it to the pool
        super.jedis.close();
    }
}
