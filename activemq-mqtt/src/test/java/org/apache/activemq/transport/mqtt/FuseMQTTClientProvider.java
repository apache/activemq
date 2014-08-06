/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.mqtt;

import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import static org.fusesource.hawtbuf.UTF8Buffer.utf8;

public class FuseMQTTClientProvider implements MQTTClientProvider {
    private final MQTT mqtt = new MQTT();
    private BlockingConnection connection;
    @Override
    public void connect(String host) throws Exception {
        mqtt.setHost(host);
        // shut off connect retry
        mqtt.setConnectAttemptsMax(0);
        mqtt.setReconnectAttemptsMax(0);
        connection = mqtt.blockingConnection();
        connection.connect();
    }

    @Override
    public void disconnect() throws Exception {
        if (this.connection != null){
            this.connection.disconnect();
        }
    }

    @Override
    public void publish(String topic, byte[] payload, int qos) throws Exception {
        publish(topic,payload,qos,false);
    }

    @Override
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception {
        connection.publish(topic,payload, QoS.values()[qos],retained);
    }

    @Override
    public void subscribe(String topic, int qos) throws Exception {
        Topic[] topics = {new Topic(utf8(topic), QoS.values()[qos])};
        connection.subscribe(topics);
    }

    @Override
    public void unsubscribe(String topic) throws Exception {
        connection.unsubscribe(new String[]{topic});
    }

    @Override
    public byte[] receive(int timeout) throws Exception {
        byte[] result = null;
        Message message = connection.receive(timeout, TimeUnit.MILLISECONDS);
        if (message != null){
            result = message.getPayload();
            message.ack();
        }
        return result;
    }

    @Override
    public void setSslContext(SSLContext sslContext) {
        mqtt.setSslContext(sslContext);
    }

    @Override
    public void setWillMessage(String string) {
        mqtt.setWillMessage(string);
    }

    @Override
    public void setWillTopic(String topic) {
        mqtt.setWillTopic(topic);
    }

    @Override
    public void setClientId(String clientId) {
        mqtt.setClientId(clientId);
    }

    @Override
    public void kill() throws Exception {
        connection.kill();
    }

    @Override
    public void setKeepAlive(int keepAlive) throws Exception {
        mqtt.setKeepAlive((short) keepAlive);
    }
}
