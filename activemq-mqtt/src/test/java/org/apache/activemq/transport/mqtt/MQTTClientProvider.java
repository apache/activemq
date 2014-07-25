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

public interface  MQTTClientProvider {
    void connect(String host) throws Exception;
    void disconnect() throws Exception;
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws Exception;
    void publish(String topic,byte[] payload,int qos) throws Exception;
    void subscribe(String topic,int qos) throws Exception;
    void unsubscribe(String topic) throws Exception;
    byte[] receive(int timeout) throws Exception;
    void setSslContext(javax.net.ssl.SSLContext sslContext);
    void setWillMessage(String string);
    void setWillTopic(String topic);
    void setClientId(String clientId);
    void kill() throws Exception;
    void setKeepAlive(int keepAlive) throws Exception;

}
