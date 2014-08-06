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
package org.apache.activemq.web;

import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.io.Buffer;
import org.eclipse.jetty.io.ByteArrayBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class AjaxTest extends JettyTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AjaxTest.class);

    private class AjaxTestContentExchange extends ContentExchange  {
        private HashMap<String,String> headers;
        private String responseContent;

        AjaxTestContentExchange() {
            super(true);
            this.headers = new HashMap<String,String>();
            this.responseContent = "";
        }
        protected void onResponseContent( Buffer content ) {
            this.responseContent += content.toString();
        }
        protected void onResponseHeader( Buffer name, Buffer value ) {
          headers.put( name.toString(), value.toString() );
        }
        public String getJsessionId() {
            String cookie = headers.get( "Set-Cookie" );
            String[] cookie_parts = cookie.split( ";" );
            return cookie_parts[0];
        }
        public String getResponseContent() {
            return responseContent;
        }
    }

    public void assertContains( String expected, String actual ) {
        assertTrue( "'"+actual+"' does not contain expected fragment '"+expected+"'", actual.indexOf( expected ) != -1 );
    }
    public void assertResponseCount( int expected, String actual ) {
        int occurrences = StringUtils.countMatches( actual, "<response" );
        assertEquals( "Expected number of <response> elements is not correct.", expected, occurrences );
    }

    @Test(timeout = 15 * 1000)
    public void testAjaxClientReceivesMessagesWhichAreSentToQueueWhileClientIsPolling() throws Exception {
        LOG.debug( "*** testAjaxClientReceivesMessagesWhichAreSentToQueueWhileClientIsPolling ***" );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        // client 1 subscribes to a queue
        LOG.debug( "SENDING LISTEN" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=queue://test&type=listen&message=handler") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );

        // while client 1 is polling, client 2 sends messages to the queue
        LOG.debug( "SENDING MESSAGES" );
        contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer(
            "destination=queue://test&type=send&message=msg1&"+
            "d1=queue://test&t1=send&m1=msg2&"+
            "d2=queue://test&t2=send&m2=msg3"
        ) );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        LOG.debug( "DONE POSTING MESSAGES" );

        // wait for poll to finish
        poll.waitForDone();
        String response = poll.getResponseContent();

        // messages might not all be delivered during the 1st poll.  We need to check again.
        poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        String fullResponse = response + poll.getResponseContent();
        LOG.debug( "full response : " + fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >msg1</response>", fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >msg2</response>", fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >msg3</response>", fullResponse );
        assertResponseCount( 3, fullResponse );

        httpClient.stop();
}

    @Test(timeout = 15 * 1000)
    public void testAjaxClientReceivesMessagesWhichAreSentToTopicWhileClientIsPolling() throws Exception {
        LOG.debug( "*** testAjaxClientReceivesMessagesWhichAreSentToTopicWhileClientIsPolling ***" );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        // client 1 subscribes to a queue
        LOG.debug( "SENDING LISTEN" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=topic://test&type=listen&message=handler") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );

        // while client 1 is polling, client 2 sends messages to the queue
        LOG.debug( "SENDING MESSAGES" );
        contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer(
            "destination=topic://test&type=send&message=msg1&"+
            "d1=topic://test&t1=send&m1=msg2&"+
            "d2=topic://test&t2=send&m2=msg3"
        ) );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        LOG.debug( "DONE POSTING MESSAGES" );

        // wait for poll to finish
        poll.waitForDone();
        String response = poll.getResponseContent();

        // not all messages might be delivered during the 1st poll.  We need to check again.
        poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        String fullResponse = response + poll.getResponseContent();
        LOG.debug( "full response : " + fullResponse );

        assertContains( "<response id='handler' destination='topic://test' >msg1</response>", fullResponse );
        assertContains( "<response id='handler' destination='topic://test' >msg2</response>", fullResponse );
        assertContains( "<response id='handler' destination='topic://test' >msg3</response>", fullResponse );
        assertResponseCount( 3, fullResponse );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testAjaxClientReceivesMessagesWhichAreQueuedBeforeClientSubscribes() throws Exception {
        LOG.debug( "*** testAjaxClientReceivesMessagesWhichAreQueuedBeforeClientSubscribes ***" );
        // send messages to queue://test
        producer.send( session.createTextMessage("test one") );
        producer.send( session.createTextMessage("test two") );
        producer.send( session.createTextMessage("test three") );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        // client 1 subscribes to queue
        LOG.debug( "SENDING LISTEN" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=queue://test&type=listen&message=handler") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );

        // wait for poll to finish
        poll.waitForDone();
        String response = poll.getResponseContent();

        assertContains( "<response id='handler' destination='queue://test' >test one</response>", response );
        assertContains( "<response id='handler' destination='queue://test' >test two</response>", response );
        assertContains( "<response id='handler' destination='queue://test' >test three</response>", response );
        assertResponseCount( 3, response );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testStompMessagesAreReceivedByAjaxClient() throws Exception {
        LOG.debug( "*** testStompMessagesAreRecievedByAjaxClient ***" );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        // client 1 subscribes to a queue
        LOG.debug( "SENDING LISTEN" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=queue://test&type=listen&message=handler") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );

        // stomp client queues some messages
        StompConnection connection = new StompConnection();
        connection.open(stompUri.getHost(), stompUri.getPort());
        connection.connect("user", "password");
        HashMap<String, String> headers = new HashMap<String, String>();
        headers.put( "amq-msg-type", "text" );
        connection.send( "/queue/test", "message1", (String)null, headers );
        connection.send( "/queue/test", "message2", (String)null, headers );
        connection.send( "/queue/test", "message3", (String)null, headers );
        connection.send( "/queue/test", "message4", (String)null, headers );
        connection.send( "/queue/test", "message5", (String)null, headers );
        String frame = "DISCONNECT\n" + "\n\n" + Stomp.NULL;
        connection.sendFrame(frame);

        // Need to let the transport have enough time to dispatch the incoming messages from
        // the socket before we break the connection.
        TimeUnit.SECONDS.sleep(5);

        connection.disconnect();

        // wait for poll to finish
        poll.waitForDone();
        String response = poll.getResponseContent();

        // not all messages might be delivered during the 1st poll.  We need to check again.
        poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        String fullResponse = response + poll.getResponseContent();

        assertContains( "<response id='handler' destination='queue://test' >message1</response>", fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >message2</response>", fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >message3</response>", fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >message4</response>", fullResponse );
        assertContains( "<response id='handler' destination='queue://test' >message5</response>", fullResponse );
        assertResponseCount( 5, fullResponse );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testAjaxMessagesAreReceivedByStompClient() throws Exception {
        LOG.debug( "*** testAjaxMessagesAreReceivedByStompClient ***" );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer(
            "destination=queue://test&type=send&message=msg1&"+
            "d1=queue://test&t1=send&m1=msg2&"+
            "d2=queue://test&t2=send&m2=msg3&"+
            "d3=queue://test&t3=send&m3=msg4") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();

        StompConnection connection = new StompConnection();
        connection.open(stompUri.getHost(), stompUri.getPort());
        connection.connect("user", "password");
        connection.subscribe( "/queue/test" );

        StompFrame message;
        String allMessageBodies = "";
        try {
            while( true ) {
                message = connection.receive(5000);
                allMessageBodies = allMessageBodies +"\n"+ message.getBody();
            }
        } catch (SocketTimeoutException e) {}

        LOG.debug( "All message bodies : " + allMessageBodies );

        assertContains( "msg1", allMessageBodies );
        assertContains( "msg2", allMessageBodies );
        assertContains( "msg3", allMessageBodies );
        assertContains( "msg4", allMessageBodies );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testAjaxClientMayUseSelectors() throws Exception {
        LOG.debug( "*** testAjaxClientMayUseSelectors ***" );

        // send 2 messages to the same queue w/ different 'filter' values.
        Message msg = session.createTextMessage("test one");
        msg.setStringProperty( "filter", "one" );
        producer.send( msg );
        msg = session.createTextMessage("test two");
        msg.setStringProperty( "filter", "two" );
        producer.send( msg );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        // client subscribes to queue
        LOG.debug( "SENDING LISTEN" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=queue://test&type=listen&message=handler") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        // SELECTOR
        contentExchange.setRequestHeader( "selector", "filter='two'" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        LOG.debug( poll.getResponseContent() );

        String expected = "<response id='handler' destination='queue://test' >test two</response>";
        assertContains( expected, poll.getResponseContent() );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testMultipleAjaxClientsMayExistInTheSameSession() throws Exception {
        LOG.debug( "*** testMultipleAjaxClientsMayExistInTheSameSession ***" );

        // send messages to queues testA and testB.
        MessageProducer producerA = session.createProducer(session.createQueue("testA"));
        MessageProducer producerB = session.createProducer(session.createQueue("testB"));
        producerA.send( session.createTextMessage("A1") );
        producerA.send( session.createTextMessage("A2") );
        producerB.send( session.createTextMessage("B1") );
        producerB.send( session.createTextMessage("B2") );

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        // clientA subscribes to /queue/testA
        LOG.debug( "SENDING LISTEN" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer(
            "destination=queue://testA&"+
            "type=listen&"+
            "message=handlerA&"+
            "clientId=clientA"
        ) );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        // clientB subscribes to /queue/testB using the same JSESSIONID.
        contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestHeader( "Cookie", jsessionid );
        contentExchange.setRequestContent( new ByteArrayBuffer(
            "destination=queue://testB&"+
            "type=listen&"+
            "message=handlerB&"+
            "clientId=clientB"
        ) );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();

        // clientA polls for messages
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000&clientId=clientA");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        LOG.debug( "clientA response : " + poll.getResponseContent() );
        String expected1 = "<response id='handlerA' destination='queue://testA' >A1</response>";
        String expected2 = "<response id='handlerA' destination='queue://testA' >A2</response>";

        assertContains( expected1, poll.getResponseContent() );
        assertContains( expected2, poll.getResponseContent() );

        // clientB polls for messages
        poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000&clientId=clientB");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        LOG.debug( "clientB response : " + poll.getResponseContent() );
        expected1 =  "<response id='handlerB' destination='queue://testB' >B1</response>";
        expected2 = "<response id='handlerB' destination='queue://testB' >B2</response>";
        assertContains( expected1, poll.getResponseContent() );
        assertContains( expected2, poll.getResponseContent() );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testAjaxClientReceivesMessagesForMultipleTopics() throws Exception {
        LOG.debug( "*** testAjaxClientReceivesMessagesForMultipleTopics ***" );
        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        LOG.debug( "SENDING LISTEN FOR /topic/topicA" );
        AjaxTestContentExchange contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=topic://topicA&type=listen&message=handlerA") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        String jsessionid = contentExchange.getJsessionId();

        LOG.debug( "SENDING LISTEN FOR /topic/topicB" );
        contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer("destination=topic://topicB&type=listen&message=handlerB") );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        contentExchange.setRequestHeader( "Cookie", jsessionid );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        AjaxTestContentExchange poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );

        // while client 1 is polling, client 2 sends messages to the topics
        LOG.debug( "SENDING MESSAGES" );
        contentExchange = new AjaxTestContentExchange();
        contentExchange.setMethod( "POST" );
        contentExchange.setURL("http://localhost:8080/amq");
        contentExchange.setRequestContent( new ByteArrayBuffer(
            "destination=topic://topicA&type=send&message=A1&"+
            "d1=topic://topicB&t1=send&m1=B1&"+
            "d2=topic://topicA&t2=send&m2=A2&"+
            "d3=topic://topicB&t3=send&m3=B2"
        ) );
        contentExchange.setRequestContentType( "application/x-www-form-urlencoded; charset=UTF-8" );
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        LOG.debug( "DONE POSTING MESSAGES" );

        // wait for poll to finish
        poll.waitForDone();
        String response = poll.getResponseContent();

        // not all messages might be delivered during the 1st poll.  We need to check again.
        poll = new AjaxTestContentExchange();
        poll.setMethod( "GET" );
        poll.setURL("http://localhost:8080/amq?timeout=5000");
        poll.setRequestHeader( "Cookie", jsessionid );
        httpClient.send( poll );
        poll.waitForDone();

        String fullResponse = response + poll.getResponseContent();
        LOG.debug( "full response " + fullResponse );
        assertContains( "<response id='handlerA' destination='topic://topicA' >A1</response>", fullResponse );
        assertContains( "<response id='handlerB' destination='topic://topicB' >B1</response>", fullResponse );
        assertContains( "<response id='handlerA' destination='topic://topicA' >A2</response>", fullResponse );
        assertContains( "<response id='handlerB' destination='topic://topicB' >B2</response>", fullResponse );
        assertResponseCount( 4, fullResponse );

        httpClient.stop();
     }
}
