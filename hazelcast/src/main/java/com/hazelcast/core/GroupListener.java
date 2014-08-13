package com.hazelcast.core;

/**
 * Interface to group listeners enabling the {@link com.hazelcast.spi.EventService} to dispatch the message to only one
 * of the listeners in the same group.
 *
 * A Listener returning a null group name is equivalent to a listener not being part
 * of a group (eg not implementing the interface). The client should assume that the message will be dispatched to
 * an undetermined listener, amongst those registered.
 *
 * @author smartrics
 */
public interface GroupListener {

    /**
     * @return the group name the listener is part of.
     */
    String getGroupName();

}
