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
package org.apache.activemq.transport.ws;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.ClientUpgradeRequest;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.UTF8Buffer;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.fusesource.mqtt.codec.CONNECT;
import org.fusesource.mqtt.codec.PUBACK;
import org.fusesource.mqtt.codec.PUBLISH;
import org.fusesource.mqtt.codec.SUBSCRIBE;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * This shows that last will and testament messages work with MQTT over WS.
 * This test is modeled after org.apache.activemq.transport.mqtt.MQTTWillTest
 */
@RunWith(Parameterized.class)
public class MQTTWSTransportWillTest extends WSTransportTestSupport {

    protected WebSocketClient wsClient;
    protected MQTTWSConnection wsMQTTConnection1;
    protected MQTTWSConnection wsMQTTConnection2;
    protected ClientUpgradeRequest request;

    private String willTopic = "willTopic";
    private String payload = "last will";
    private boolean closeWithDisconnect;

    //Test both with a proper disconnect and without
    @Parameters(name="closeWithDisconnect={0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true},
                {false}
            });
    }

    public MQTTWSTransportWillTest(boolean closeWithDisconnect) {
        this.closeWithDisconnect = closeWithDisconnect;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        //turn off advisory support
        broker = createBroker(true, false);

        wsClient = new WebSocketClient(new SslContextFactory(true));
        wsClient.start();

        request = new ClientUpgradeRequest();
        request.setSubProtocols("mqttv3.1");

        wsMQTTConnection1 = new MQTTWSConnection();
        wsMQTTConnection2 = new MQTTWSConnection();

        wsClient.connect(wsMQTTConnection1, wsConnectUri, request);
        if (!wsMQTTConnection1.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to MQTT WS endpoint");
        }

        wsClient.connect(wsMQTTConnection2, wsConnectUri, request);
        if (!wsMQTTConnection2.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to MQTT WS endpoint");
        }
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (wsMQTTConnection1 != null) {
            wsMQTTConnection1.close();
            wsMQTTConnection1 = null;
        }
        if (wsMQTTConnection2 != null) {
            wsMQTTConnection2.close();
            wsMQTTConnection2 = null;
        }
        wsClient.stop();
        wsClient = null;
        super.tearDown();
    }

    @Test(timeout = 60000)
    public void testWill() throws Exception {

        //connect with will retain false
        CONNECT command = getWillConnectCommand(false);

        //connect both connections
        wsMQTTConnection1.connect(command);
        wsMQTTConnection2.connect();

        //Subscribe to topics
        SUBSCRIBE subscribe = new SUBSCRIBE();
        subscribe.topics(new Topic[] {new Topic("#", QoS.EXACTLY_ONCE) });
        wsMQTTConnection2.sendFrame(subscribe.encode());
        wsMQTTConnection2.receive(5, TimeUnit.SECONDS);

        //Test message send/receive
        wsMQTTConnection1.sendFrame(getTestMessage((short) 125).encode());
        assertMessageReceived(wsMQTTConnection2);

        //close the first connection without sending a proper disconnect frame first
        //if closeWithDisconnect is false
        if (closeWithDisconnect) {
            wsMQTTConnection1.disconnect();
        }
        wsMQTTConnection1.close();

        //Make sure LWT message is not received
        if (closeWithDisconnect) {
            assertNull(wsMQTTConnection2.receive(5, TimeUnit.SECONDS));
        //make sure LWT is received
        } else {
            assertWillTopicReceived(wsMQTTConnection2);
        }
    }

    @Test(timeout = 60 * 1000)
    public void testRetainWillMessage() throws Exception {

        //create connection with will retain true
        CONNECT command = getWillConnectCommand(true);

        wsMQTTConnection1.connect(command);
        wsMQTTConnection2.connect();

        //set to at most once to test will retain
        SUBSCRIBE subscribe = new SUBSCRIBE();
        subscribe.topics(new Topic[] {new Topic("#", QoS.AT_MOST_ONCE) });
        wsMQTTConnection2.sendFrame(subscribe.encode());
        wsMQTTConnection2.receive(5, TimeUnit.SECONDS);

        //Test message send/receive
        PUBLISH pub = getTestMessage((short) 127);
        wsMQTTConnection1.sendFrame(pub.encode());
        assertMessageReceived(wsMQTTConnection2);
        PUBACK ack = new PUBACK();
        ack.messageId(pub.messageId());
        wsMQTTConnection2.sendFrame(ack.encode());

        //Properly close connection 2 and improperly close connection 1 for LWT test
        wsMQTTConnection2.disconnect();
        wsMQTTConnection2.close();
        Thread.sleep(1000);
        //close the first connection without sending a proper disconnect frame first
        //if closeWithoutDisconnect is false
        if (closeWithDisconnect) {
            wsMQTTConnection1.disconnect();
        }
        wsMQTTConnection1.close();
        Thread.sleep(1000);

        //Do the reconnect of the websocket after close
        wsMQTTConnection2 = new MQTTWSConnection();
        wsClient.connect(wsMQTTConnection2, wsConnectUri, request);
        if (!wsMQTTConnection2.awaitConnection(30, TimeUnit.SECONDS)) {
            throw new IOException("Could not connect to MQTT WS endpoint");
        }


        //Make sure the will message is received on reconnect
        wsMQTTConnection2.connect();
        wsMQTTConnection2.sendFrame(subscribe.encode());
        wsMQTTConnection2.receive(5, TimeUnit.SECONDS);

        //Make sure LWT message not received
        if (closeWithDisconnect) {
            assertNull(wsMQTTConnection2.receive(5, TimeUnit.SECONDS));
        //make sure LWT is received
        } else {
            assertWillTopicReceived(wsMQTTConnection2);
        }
    }

    private PUBLISH getTestMessage(short id) {
        PUBLISH publish = new PUBLISH();
        publish.dup(false);
        publish.messageId(id);
        publish.qos(QoS.AT_LEAST_ONCE);
        publish.payload(new Buffer("hello world".getBytes()));
        publish.topicName(new UTF8Buffer("test"));

        return publish;
    }

    private CONNECT getWillConnectCommand(boolean willRetain) {
        CONNECT command = new CONNECT();
        command.clientId(new UTF8Buffer("clientId"));
        command.cleanSession(false);
        command.version(3);
        command.keepAlive((short) 0);
        command.willMessage(new UTF8Buffer(payload));
        command.willQos(QoS.AT_LEAST_ONCE);
        command.willTopic(new UTF8Buffer(willTopic));
        command.willRetain(willRetain);

        return command;
    }

    private void assertMessageReceived(MQTTWSConnection wsMQTTConnection2) throws Exception {
        PUBLISH msg = new PUBLISH();
        msg.decode(wsMQTTConnection2.receive(5, TimeUnit.SECONDS));
        assertNotNull(msg);
        assertEquals("hello world", msg.payload().ascii().toString());
        assertEquals("test", msg.topicName().toString());
    }

    private void assertWillTopicReceived(MQTTWSConnection wsMQTTConnection2) throws Exception {
        PUBLISH willMsg = new PUBLISH();
        willMsg.decode(wsMQTTConnection2.receive(5, TimeUnit.SECONDS));
        assertNotNull(willMsg);
        assertEquals(payload, willMsg.payload().ascii().toString());
        assertEquals(willTopic,  willMsg.topicName().toString());
    }

}
