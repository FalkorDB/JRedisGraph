package com.redislabs.redisgraph.impl.api;

import com.redislabs.redisgraph.Connection;

import redis.clients.jedis.Client;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * Cluster Connection implementation of the Connection interface for RedisGraph
 */
public class ClusterConnection implements Connection {
    private final JedisCluster cluster;

    public ClusterConnection(JedisCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public Object sendCommand(ProtocolCommand cmd, String... args) {
        return cluster.sendCommand(args[0], cmd, args);
    }

    @Override
    public Object sendBlockingCommand(ProtocolCommand cmd, String... args) {
        return cluster.sendBlockingCommand(args[0], cmd, args);
    }

    @Override
    public void close() {
        // Doesn't actually close the connection
    }

    @Override
    public String watch(String... keys) {
        throw new UnsupportedOperationException("Cluster does not support watch");
    }

    @Override
    public String unwatch() {
        throw new UnsupportedOperationException("Cluster does not support unwatch");
    }

    @Override
    public Client getClient() {
        throw new UnsupportedOperationException("Cluster does not support pipeline");
    }

    @Override
    public void disconnect() {
        cluster.close();
    }  
}
