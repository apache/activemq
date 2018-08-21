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

import java.net.Socket;
import java.net.URI;

import javax.management.ObjectName;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class AMQ4126Test {

    protected BrokerService broker;

    protected String java_security_auth_login_config = "java.security.auth.login.config";
    protected String xbean = "xbean:";
    protected String confBase = "src/test/resources/org/apache/activemq/bugs/amq4126";
    protected String certBase = "src/test/resources/org/apache/activemq/security";
    protected String JaasStompSSLBroker_xml = "JaasStompSSLBroker.xml";
    protected StompConnection stompConnection = new StompConnection();
    private final static String destinationName = "TEST.QUEUE";
    protected String oldLoginConf = null;

    @Before
    public void before() throws Exception {
        if (System.getProperty(java_security_auth_login_config) != null) {
            oldLoginConf = System.getProperty(java_security_auth_login_config);
        }
        System.setProperty(java_security_auth_login_config, confBase + "/login.config");
        broker = BrokerFactory.createBroker(xbean + confBase + "/" + JaasStompSSLBroker_xml);

        broker.setDeleteAllMessagesOnStartup(true);
        broker.setUseJmx(true);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        broker.stop();

        if (oldLoginConf != null) {
            System.setProperty(java_security_auth_login_config, oldLoginConf);
        }
    }

    public Socket createSocket(String host, int port) throws Exception {
        System.setProperty("javax.net.ssl.trustStore", certBase + "/broker1.ks");
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
        System.setProperty("javax.net.ssl.keyStore", certBase + "/client.ks");
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");

        SocketFactory factory = SSLSocketFactory.getDefault();
        return factory.createSocket(host, port);
    }

    public void stompConnectTo(String connectorName, String extraHeaders) throws Exception {
        String host = broker.getConnectorByName(connectorName).getConnectUri().getHost();
        int port = broker.getConnectorByName(connectorName).getConnectUri().getPort();
        stompConnection.open(createSocket(host, port));
        String extra = extraHeaders != null ? extraHeaders : "\n";
        stompConnection.sendFrame("CONNECT\n" + extra + "\n" + Stomp.NULL);

        StompFrame f = stompConnection.receive();
        TestCase.assertEquals(f.getBody(), "CONNECTED", f.getAction());
        stompConnection.close();
    }

    @Test
    public void testStompSSLWithUsernameAndPassword() throws Exception {
        stompConnectTo("stomp+ssl", "login:system\n" + "passcode:manager\n");
    }

    @Test
    public void testStompSSLWithCertificate() throws Exception {
        stompConnectTo("stomp+ssl", null);
    }

    @Test
    public void testStompNIOSSLWithUsernameAndPassword() throws Exception {
        stompConnectTo("stomp+nio+ssl", "login:system\n" + "passcode:manager\n");
    }

    @Test
    public void testStompNIOSSLWithCertificate() throws Exception {
        stompConnectTo("stomp+nio+ssl", null);
    }

    public void openwireConnectTo(String connectorName, String username, String password) throws Exception {
        URI brokerURI = broker.getConnectorByName(connectorName).getConnectUri();
        String uri = "ssl://" + brokerURI.getHost() + ":" + brokerURI.getPort() + "?socket.verifyHostName=false";
        ActiveMQSslConnectionFactory cf = new ActiveMQSslConnectionFactory(uri);
        cf.setTrustStore("org/apache/activemq/security/broker1.ks");
        cf.setTrustStorePassword("password");
        cf.setKeyStore("org/apache/activemq/security/client.ks");
        cf.setKeyStorePassword("password");
        ActiveMQConnection connection = null;
        if (username != null || password != null) {
            connection = (ActiveMQConnection)cf.createConnection(username, password);
        } else {
            connection = (ActiveMQConnection)cf.createConnection();
        }
        TestCase.assertNotNull(connection);
        connection.start();
        connection.stop();
    }

    @Test
    public void testOpenwireSSLWithUsernameAndPassword() throws Exception {
        openwireConnectTo("openwire+ssl", "system", "manager");
    }

    @Test
    public void testOpenwireSSLWithCertificate() throws Exception {
        openwireConnectTo("openwire+ssl", null, null);
    }

    @Test
    public void testOpenwireNIOSSLWithUsernameAndPassword() throws Exception {
        openwireConnectTo("openwire+nio+ssl", "system", "mmanager");
    }

    @Test
    public void testOpenwireNIOSSLWithCertificate() throws Exception {
        openwireConnectTo("openwire+nio+ssl", null, null);
    }

    @Test
    public void testJmx() throws Exception {
        TestCase.assertFalse(findDestination(destinationName));
        broker.getAdminView().addQueue(destinationName);
        TestCase.assertTrue(findDestination(destinationName));
        broker.getAdminView().removeQueue(destinationName);
        TestCase.assertFalse(findDestination(destinationName));
    }

    private boolean findDestination(String name) throws Exception {
        ObjectName[] destinations = broker.getAdminView().getQueues();
        for (ObjectName destination : destinations) {
            if (destination.toString().contains(name)) {
                return true;
            }
        }
        return false;
    }

}
