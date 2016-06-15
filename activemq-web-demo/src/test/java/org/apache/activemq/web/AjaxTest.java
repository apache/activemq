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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.Message;
import javax.jms.MessageProducer;

import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;
import org.apache.activemq.transport.stomp.StompFrame;
import org.apache.commons.lang.StringUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AjaxTest extends JettyTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AjaxTest.class);


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
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        // client 1 subscribes to a queue
        LOG.debug( "SENDING LISTEN" );
        String sessionId = subscribe(httpClient, port, "destination=queue://test&type=listen&message=handler");

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf, sessionId);

        // while client 1 is polling, client 2 sends messages to the queue
        LOG.debug( "SENDING MESSAGES" );
        sendMessages(httpClient, port, ("destination=queue://test&type=send&message=msg1&"+
                         "d1=queue://test&t1=send&m1=msg2&"+
                         "d2=queue://test&t2=send&m2=msg3").getBytes());

        LOG.debug( "DONE POSTING MESSAGES" );

        // wait for poll to finish
       latch.await();
       String response = buf.toString();

        // messages might not all be delivered during the 1st poll.  We need to check again.
       final StringBuffer buf2 = new StringBuffer();
       final CountDownLatch latch2 =
               asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf2, sessionId);
       latch2.await();

        String fullResponse = response + buf2.toString();
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
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        // client 1 subscribes to a queue
        LOG.debug( "SENDING LISTEN" );
        String sessionId = subscribe(httpClient, port, "destination=topic://test&type=listen&message=handler");

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf, sessionId);

        // while client 1 is polling, client 2 sends messages to the queue
        LOG.debug( "SENDING MESSAGES" );
        sendMessages(httpClient, port, ("destination=topic://test&type=send&message=msg1&"+
                "d1=topic://test&t1=send&m1=msg2&"+
                "d2=topic://test&t2=send&m2=msg3").getBytes());

        // wait for poll to finish
        latch.await();
        String response = buf.toString();

        // messages might not all be delivered during the 1st poll. We need to
        // check again.
        final StringBuffer buf2 = new StringBuffer();
        final CountDownLatch latch2 = asyncRequest(httpClient,
                "http://localhost:" + port + "/amq?timeout=5000", buf2, sessionId);
        latch2.await();

        String fullResponse = response + buf2.toString();

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
        int port = getPort();

        // send messages to queue://test
        producer.send( session.createTextMessage("test one") );
        producer.send( session.createTextMessage("test two") );
        producer.send( session.createTextMessage("test three") );

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        // client 1 subscribes to queue
        LOG.debug( "SENDING LISTEN" );
        String sessionId = subscribe(httpClient, port, "destination=queue://test&type=listen&message=handler");

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf, sessionId);

        // wait for poll to finish
        latch.await();
        String response = buf.toString();

        assertContains( "<response id='handler' destination='queue://test' >test one</response>", response );
        assertContains( "<response id='handler' destination='queue://test' >test two</response>", response );
        assertContains( "<response id='handler' destination='queue://test' >test three</response>", response );
        assertResponseCount( 3, response );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testStompMessagesAreReceivedByAjaxClient() throws Exception {
        LOG.debug( "*** testStompMessagesAreRecievedByAjaxClient ***" );
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        // client 1 subscribes to a queue
        LOG.debug( "SENDING LISTEN" );
        String sessionId = subscribe(httpClient, port, "destination=queue://test&type=listen&message=handler");

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf, sessionId);

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
        latch.await();
        String response = buf.toString();

        // not all messages might be delivered during the 1st poll.  We need to check again.
        final StringBuffer buf2 = new StringBuffer();
        final CountDownLatch latch2 =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf2, sessionId);
        latch2.await();

        String fullResponse = response + buf2.toString();

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
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        sendMessages(httpClient, port, ("destination=queue://test&type=send&message=msg1&"+
                "d1=queue://test&t1=send&m1=msg2&"+
                "d2=queue://test&t2=send&m2=msg3&"+
                "d3=queue://test&t3=send&m3=msg4").getBytes());

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
        int port = getPort();


        // send 2 messages to the same queue w/ different 'filter' values.
        Message msg = session.createTextMessage("test one");
        msg.setStringProperty( "filter", "one" );
        producer.send( msg );
        msg = session.createTextMessage("test two");
        msg.setStringProperty( "filter", "two" );
        producer.send( msg );

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        // client subscribes to queue
        LOG.debug( "SENDING LISTEN" );
        String sessionId = subscribe(httpClient, port, "destination=queue://test&type=listen&message=handler", "filter='two'", null);

        // client 1 polls for messages
        LOG.debug( "SENDING POLL" );
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf, sessionId);
        latch.await();

        LOG.debug( buf.toString() );

        String expected = "<response id='handler' destination='queue://test' >test two</response>";
        assertContains( expected, buf.toString() );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testMultipleAjaxClientsMayExistInTheSameSession() throws Exception {
        LOG.debug( "*** testMultipleAjaxClientsMayExistInTheSameSession ***" );
        int port = getPort();

        // send messages to queues testA and testB.
        MessageProducer producerA = session.createProducer(session.createQueue("testA"));
        MessageProducer producerB = session.createProducer(session.createQueue("testB"));
        producerA.send( session.createTextMessage("A1") );
        producerA.send( session.createTextMessage("A2") );
        producerB.send( session.createTextMessage("B1") );
        producerB.send( session.createTextMessage("B2") );

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        // clientA subscribes to /queue/testA
        LOG.debug( "SENDING LISTEN" );
        String sessionId = subscribe(httpClient, port, "destination=queue://testA&type=listen&message=handlerA&clientId=clientA");

        // clientB subscribes to /queue/testB using the same JSESSIONID.
        subscribe(httpClient, port, "destination=queue://testB&type=listen&message=handlerB&clientId=clientB", null, sessionId);

        // clientA polls for messages
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000&clientId=clientA", buf, sessionId);
        latch.await();

        LOG.debug( "clientA response : " + buf.toString() );
        String expected1 = "<response id='handlerA' destination='queue://testA' >A1</response>";
        String expected2 = "<response id='handlerA' destination='queue://testA' >A2</response>";

        assertContains( expected1, buf.toString() );
        assertContains( expected2, buf.toString() );

        // clientB polls for messages
        final StringBuffer buf2 = new StringBuffer();
        final CountDownLatch latch2 =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000&clientId=clientB", buf2, sessionId);
        latch2.await();

        LOG.debug( "clientB response : " + buf2.toString() );
        expected1 =  "<response id='handlerB' destination='queue://testB' >B1</response>";
        expected2 = "<response id='handlerB' destination='queue://testB' >B2</response>";
        assertContains( expected1, buf2.toString() );
        assertContains( expected2, buf2.toString() );

        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testAjaxClientReceivesMessagesForMultipleTopics() throws Exception {
        LOG.debug( "*** testAjaxClientReceivesMessagesForMultipleTopics ***" );
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        LOG.debug( "SENDING LISTEN FOR /topic/topicA" );
        String sessionId = subscribe(httpClient, port, "destination=topic://topicA&type=listen&message=handlerA");


        LOG.debug( "SENDING LISTEN FOR /topic/topicB" );
        subscribe(httpClient, port, "destination=topic://topicB&type=listen&message=handlerB", null, sessionId);

        // client 1 polls for messages
        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf, sessionId);

        // while client 1 is polling, client 2 sends messages to the topics
        LOG.debug( "SENDING MESSAGES" );
        sendMessages(httpClient, port, ("destination=topic://topicA&type=send&message=A1&"+
                "d1=topic://topicB&t1=send&m1=B1&"+
                "d2=topic://topicA&t2=send&m2=A2&"+
                "d3=topic://topicB&t3=send&m3=B2").getBytes());
        LOG.debug( "DONE POSTING MESSAGES" );

        // wait for poll to finish
        latch.await();
        String response = buf.toString();

        // not all messages might be delivered during the 1st poll.  We need to check again.
        final StringBuffer buf2 = new StringBuffer();
        final CountDownLatch latch2 =
                asyncRequest(httpClient, "http://localhost:" + port + "/amq?timeout=5000", buf2, sessionId);
        latch2.await();

        String fullResponse = response + buf2.toString();
        LOG.debug( "full response " + fullResponse );
        assertContains( "<response id='handlerA' destination='topic://topicA' >A1</response>", fullResponse );
        assertContains( "<response id='handlerB' destination='topic://topicB' >B1</response>", fullResponse );
        assertContains( "<response id='handlerA' destination='topic://topicA' >A2</response>", fullResponse );
        assertContains( "<response id='handlerB' destination='topic://topicB' >B2</response>", fullResponse );
        assertResponseCount( 4, fullResponse );

        httpClient.stop();
     }

    protected void sendMessages(HttpClient httpClient, int port, byte[] content) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        httpClient
                .newRequest("http://localhost:" + port + "/amq")
                .header("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .content(
                        new InputStreamContentProvider(new ByteArrayInputStream(content)))
                .method(HttpMethod.POST).send(new BufferingResponseListener() {
                    @Override
                    public void onComplete(Result result) {
                        buf.append(getContentAsString());
                        latch.countDown();
                    }
                });
        latch.await();
    }

    protected String subscribe(HttpClient httpClient, int port, String content) throws InterruptedException {
        return this.subscribe(httpClient, port, content, null, null);
    }

    protected String subscribe(HttpClient httpClient, int port, String content, String selector, String session) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        final StringBuffer sessionId = new StringBuffer();
        Request request = httpClient.newRequest("http://localhost:" + port + "/amq");
        if (selector != null) {
            request.header("selector", selector);
        }
        if (session != null) {
            request.header(HttpHeader.COOKIE, session);
        }
        request.header("Content-Type","application/x-www-form-urlencoded; charset=UTF-8")
           .content(new InputStreamContentProvider(new ByteArrayInputStream(content.getBytes())))
           .method(HttpMethod.POST).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                buf.append(getContentAsString());
                String cookie = result.getResponse().getHeaders().get(HttpHeader.SET_COOKIE);
                if (cookie != null) {
                    String[] cookieParts = cookie.split(";");
                    sessionId.append(cookieParts[0]);
                }
                latch.countDown();
            }
        });
        latch.await();

        return sessionId.toString();
    }

    protected CountDownLatch asyncRequest(final HttpClient httpClient, final String url, final StringBuffer buffer,
            final String sessionId) {
        final CountDownLatch latch = new CountDownLatch(1);
        Request request = httpClient.newRequest(url);
        if (sessionId != null) {
            request.header(HttpHeader.COOKIE, sessionId);
        }
        request.send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                buffer.append(getContentAsString());
                latch.countDown();
            }
        });
        return latch;
    }

    protected CountDownLatch asyncRequest(final HttpClient httpClient, final String url, final StringBuffer buffer,
            final AtomicInteger status) {
        final CountDownLatch latch = new CountDownLatch(1);
        httpClient.newRequest(url).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                status.getAndSet(result.getResponse().getStatus());
                buffer.append(getContentAsString());
                latch.countDown();
            }
        });
        return latch;
    }
}
