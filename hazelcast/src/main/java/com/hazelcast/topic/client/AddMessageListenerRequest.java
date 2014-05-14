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

package com.hazelcast.topic.client;

import com.hazelcast.client.CallableClientRequest;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.RetryableRequest;
import com.hazelcast.client.SecureRequest;
import com.hazelcast.core.GroupListener;
import com.hazelcast.core.Message;
import com.hazelcast.core.MessageListener;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.topic.TopicPortableHook;
import com.hazelcast.topic.TopicService;

import java.io.IOException;
import java.security.Permission;

public class AddMessageListenerRequest extends CallableClientRequest
        implements Portable, SecureRequest, RetryableRequest {

    private String listenerGroup;
    private String name;

    public AddMessageListenerRequest() {
    }

    public AddMessageListenerRequest(String name) {
        this.name = name;
        this.listenerGroup = null;
    }

    public AddMessageListenerRequest(String name, String listenerGroup) {
        this.name = name;
        this.listenerGroup = listenerGroup;
    }

    @Override
    public String call() throws Exception {
        TopicService service = getService();
        ClientEngine clientEngine = getClientEngine();
        ClientEndpoint endpoint = getEndpoint();
        MessageListener listener = new MessageListenerImpl(endpoint, clientEngine, getCallId(), listenerGroup);

        String registrationId = service.addMessageListener(name, listener);
        endpoint.setListenerRegistration(TopicService.SERVICE_NAME, name, registrationId);
        return registrationId;
    }

    @Override
    public String getServiceName() {
        return TopicService.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return TopicPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return TopicPortableHook.ADD_LISTENER;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("lg", listenerGroup);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        listenerGroup = reader.readUTF("lg");
    }

    @Override
    public Permission getRequiredPermission() {
        return new TopicPermission(name, ActionConstants.ACTION_LISTEN);
    }

    private static class MessageListenerImpl implements MessageListener, GroupListener {
        private final ClientEndpoint endpoint;
        private final ClientEngine clientEngine;
        private final int callId;
        private final String groupName;

        public MessageListenerImpl(ClientEndpoint endpoint, ClientEngine clientEngine, int callId, String groupName) {
            this.endpoint = endpoint;
            this.clientEngine = clientEngine;
            this.callId = callId;
            this.groupName = groupName;
        }

        @Override
        public void onMessage(Message message) {
            if (!endpoint.live()) {
                return;
            }
            Data messageData = clientEngine.toData(message.getMessageObject());
            String publisherUuid = message.getPublishingMember().getUuid();
            PortableMessage portableMessage = new PortableMessage(messageData, message.getPublishTime(), publisherUuid);
            endpoint.sendEvent(portableMessage, callId);
        }

        @Override
        public String getGroupName() {
            return groupName;
        }

        @Override
        public String toString() {
            return "MessageListenerImpl@" + hashCode() + " [" +
                    "endpoint=" + endpoint +
                    ", clientEngine=" + clientEngine +
                    ", callId=" + callId +
                    ", groupName='" + groupName + '\'' +
                    ']';
        }
    }
}
