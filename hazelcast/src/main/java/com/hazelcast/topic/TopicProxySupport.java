/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.topic;

import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.MessageEventFilter;
import com.hazelcast.core.MessageListener;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.monitor.LocalTopicStats;
import com.hazelcast.monitor.impl.LocalTopicStatsImpl;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InitializingObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

abstract class TopicProxySupport extends AbstractDistributedObject<TopicService> implements InitializingObject {

    private final String name;
    private final ClassLoader configClassLoader;
    private final TopicService topicService;
    private final LocalTopicStatsImpl topicStats;
    private final MemberImpl localMember;

    TopicProxySupport(String name, NodeEngine nodeEngine, TopicService service) {
        super(nodeEngine, service);
        this.name = name;
        this.configClassLoader = nodeEngine.getConfigClassLoader();
        this.topicService = service;
        this.topicStats = topicService.getLocalTopicStats(name);
        this.localMember = nodeEngine.getLocalMember();
    }

    @Override
    public void initialize() {
        NodeEngine nodeEngine = getNodeEngine();
        TopicConfig config = nodeEngine.getConfig().findTopicConfig(name);
        for (ListenerConfig listenerConfig : config.getMessageListenerConfigs()) {
            initialize(listenerConfig);
        }
    }

    private void initialize(ListenerConfig listenerConfig) {
        NodeEngine nodeEngine = getNodeEngine();

        MessageListener listener;
        try {
            listener = (MessageListener) listenerConfig.getImplementation();
            if (listener == null && listenerConfig.getClassName() != null) {
                listener = ClassLoaderUtil.newInstance(configClassLoader, listenerConfig.getClassName());
            }
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }

        if (listener != null) {
            if (listener instanceof HazelcastInstanceAware) {
                HazelcastInstanceAware hazelcastInstanceAware = (HazelcastInstanceAware) listener;
                hazelcastInstanceAware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
            }

            MessageEventFilter filter;
            try {
                filter = (MessageEventFilter) listenerConfig.getFilterImplementation();
                if(filter == null && listenerConfig.getFilterClassName() != null) {
                    filter = ClassLoaderUtil.newInstance(configClassLoader, listenerConfig.getFilterClassName());
                }
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }

            if (filter instanceof HazelcastInstanceAware) {
                HazelcastInstanceAware hazelcastInstanceAware = (HazelcastInstanceAware) filter;
                hazelcastInstanceAware.setHazelcastInstance(nodeEngine.getHazelcastInstance());
            }

            addMessageListenerInternal(listener, filter);
        }
    }

    public LocalTopicStats getLocalTopicStatsInternal() {
        return topicService.getLocalTopicStats(name);
    }

    public void publishInternal(Object messageObject, Data message) {
        TopicEvent topicEvent = new TopicEvent(name, message, localMember);
        topicStats.incrementPublishes();
        topicService.publishEvent(name, messageObject, topicEvent);
    }

    public String addMessageListenerInternal(MessageListener listener, MessageEventFilter filter) {
        return topicService.addMessageListener(name, listener, filter);
    }

    public boolean removeMessageListenerInternal(final String registrationId) {
        return topicService.removeMessageListener(name, registrationId);
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public String getName() {
        return name;
    }
}
