package com.hazelcast.core;

/**
 * Specifies a topic message filter a client can use when registering a filter to filter out messages.
 *
 * @author smartrics
 */
public interface MessageEventFilter<E> {
    boolean shouldPublish(E messageData);
}
