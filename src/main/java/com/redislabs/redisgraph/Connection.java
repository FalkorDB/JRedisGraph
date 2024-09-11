package com.redislabs.redisgraph;

import java.io.Closeable;
import redis.clients.jedis.Client;
import redis.clients.jedis.commands.ProtocolCommand;

/**
 * Connection interface
 * 
 * This interface defines the methods that a connection to a RedisGraph instance should implement.
 * It abstracts the underlying connection mechanism, allowing for different implementations to be used.
 */
public interface Connection extends Closeable {
    Object sendCommand(ProtocolCommand cmd, String... args);

    Object sendBlockingCommand(ProtocolCommand cmd, String... args);

    void close();

    String watch(String... keys);

    String unwatch();

    Client getClient();

    void disconnect();
}

