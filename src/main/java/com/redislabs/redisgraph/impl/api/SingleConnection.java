package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.Connection;

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * Single Connection implementation of the Connection interface for RedisGraph
*/
public class SingleConnection implements Connection {
    protected final Jedis jedis;

    public SingleConnection(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public Object sendCommand(ProtocolCommand cmd, String... args) {
        return jedis.sendCommand(cmd, args);
    }

    @Override
    public Object sendBlockingCommand(ProtocolCommand cmd, String... args) {
        return jedis.sendBlockingCommand(cmd, args);
    }

    @Override
    public void close() {
        // Doesn't actually close the connection
    }

    @Override
    public String watch(String... keys) {
        return jedis.watch(keys);
    }

    @Override
    public String unwatch() {
        return jedis.unwatch();
    }

    @Override
    public Client getClient() {
        return jedis.getClient();
    }

    @Override
    public void disconnect() {
        jedis.close();
    }
    
}
