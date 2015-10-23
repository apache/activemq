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
package org.apache.activemq.bugs;

import junit.framework.TestCase;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.SpringSslContext;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.fusesource.mqtt.client.MQTT;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.net.Socket;

public class AMQ4133Test {

    protected String java_security_auth_login_config = "java.security.auth.login.config";
    protected String xbean = "xbean:";
    protected String confBase = "src/test/resources/org/apache/activemq/bugs/amq4126";
    protected String certBase = "src/test/resources/org/apache/activemq/security";
    protected String activemqXml = "InconsistentConnectorPropertiesBehaviour.xml";
    protected BrokerService broker;

    protected String oldLoginConf = null;

    @Before
    public void before() throws Exception {
        if (System.getProperty(java_security_auth_login_config) != null) {
            oldLoginConf = System.getProperty(java_security_auth_login_config);
        }
        System.setProperty(java_security_auth_login_config, confBase + "/" + "login.config");
        broker = BrokerFactory.createBroker(xbean + confBase + "/" + activemqXml);

        broker.start();
        broker.waitUntilStarted();

        System.setProperty("javax.net.ssl.trustStore", certBase + "/" + "broker1.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", certBase + "/" + "client.ks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");
    }

    @After
    public void after() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void stompSSLTransportNeedClientAuthTrue() throws Exception {
        stompConnectTo("localhost", broker.getConnectorByName("stomp+ssl").getConnectUri().getPort());
    }

    @Test
    public void stompSSLNeedClientAuthTrue() throws Exception {
        stompConnectTo("localhost", broker.getConnectorByName("stomp+ssl+special").getConnectUri().getPort());
    }

    @Test
    public void stompNIOSSLTransportNeedClientAuthTrue() throws Exception {
        stompConnectTo("localhost", broker.getConnectorByName("stomp+nio+ssl").getConnectUri().getPort());
    }

    @Test
    public void stompNIOSSLNeedClientAuthTrue() throws Exception {
        stompConnectTo("localhost", broker.getConnectorByName("stomp+nio+ssl+special").getConnectUri().getPort());
    }

    @Test
    public void mqttSSLNeedClientAuthTrue() throws Exception {
        mqttConnectTo("localhost", broker.getConnectorByName("mqtt+ssl").getConnectUri().getPort());
    }

    @Test
    public void mqttNIOSSLNeedClientAuthTrue() throws Exception {
        mqttConnectTo("localhost", broker.getConnectorByName("mqtt+nio+ssl").getConnectUri().getPort());
    }

    public Socket createSocket(String host, int port) throws Exception {
        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket(host, port);
    }

    public void stompConnectTo(String host, int port) throws Exception {
        StompConnection stompConnection = new StompConnection();
        stompConnection.open(createSocket(host, port));
        stompConnection.sendFrame("CONNECT\n" + "\n" + Stomp.NULL);
        StompFrame f = stompConnection.receive();
        TestCase.assertEquals(f.getBody(), "CONNECTED", f.getAction());
        stompConnection.close();
    }

    public void mqttConnectTo(String host, int port) throws Exception {
        MQTT mqtt = new MQTT();
        mqtt.setConnectAttemptsMax(1);
        mqtt.setReconnectAttemptsMax(0);
        mqtt.setHost("tls://" + host + ":" + port);
        mqtt.setClientId("test");
        mqtt.setCleanSession(true);

        SpringSslContext context = new SpringSslContext();
        context.setKeyStore(certBase + "/" + "client.ks");
        context.setKeyStorePassword("password");
        context.setTrustStore(certBase + "/" + "broker1.ks");
        context.setTrustStorePassword("password");
        context.afterPropertiesSet();

        mqtt.setSslContext(SSLContext.getDefault());
        mqtt.blockingConnection().connect();
    }

}
