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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.commons.lang.RandomStringUtils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestTest extends JettyTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(RestTest.class);

    @Test(timeout = 60 * 1000)
    public void testConsume() throws Exception {
        int port = getPort();

        producer.send(session.createTextMessage("test"));
        LOG.info("message sent");

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/message/test?readTimeout=1000&type=queue", buf);

        latch.await();
        assertEquals("test", buf.toString());
    }

    @Test(timeout = 60 * 1000)
    public void testSubscribeFirst() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/message/test?readTimeout=5000&type=queue", buf);

        producer.send(session.createTextMessage("test"));
        LOG.info("message sent");

        latch.await();
        assertEquals("test", buf.toString());

    }

    @Test(timeout = 60 * 1000)
    public void testSelector() throws Exception {
        int port = getPort();

        TextMessage msg1 = session.createTextMessage("test1");
        msg1.setIntProperty("test", 1);
        producer.send(msg1);
        LOG.info("message 1 sent");

        TextMessage msg2 = session.createTextMessage("test2");
        msg2.setIntProperty("test", 2);
        producer.send(msg2);
        LOG.info("message 2 sent");

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch = new CountDownLatch(1);
        httpClient.newRequest("http://localhost:" + port + "/message/test?readTimeout=1000&type=queue")
            .header("selector", "test=2").send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                buf.append(getContentAsString());
                latch.countDown();
            }
        });
        latch.await();
        assertEquals("test2", buf.toString());
    }


    // test for https://issues.apache.org/activemq/browse/AMQ-2827
    @Test(timeout = 15 * 1000)
    public void testCorrelation() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        for (int i = 0; i < 200; i++) {
            String correlId = "RESTY" + RandomStringUtils.randomNumeric(10);

            TextMessage message = session.createTextMessage(correlId);
            message.setStringProperty("correlationId", correlId);
            message.setJMSCorrelationID(correlId);

            LOG.info("Sending: " + correlId);
            producer.send(message);

            final StringBuffer buf = new StringBuffer();
            final CountDownLatch latch =
                    asyncRequest(httpClient, "http://localhost:" + port + "/message/test?readTimeout=1000&type=queue&clientId=test", buf);

            latch.await();
            LOG.info("Received: " +  buf.toString());
           // assertEquals(200, contentExchange.getResponseStatus());
            assertEquals(correlId,  buf.toString());
        }
        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testDisconnect() throws Exception {
        int port = getPort();

        producer.send(session.createTextMessage("test"));
        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final StringBuffer buf = new StringBuffer();
        final CountDownLatch latch =
                asyncRequest(httpClient, "http://localhost:" + port + "/message/test?readTimeout=1000&type=queue&clientId=test", buf);

        latch.await();
        LOG.info("Received: " + buf.toString());

        final StringBuffer buf2 = new StringBuffer();
        final CountDownLatch latch2 = new CountDownLatch(1);
        httpClient.newRequest("http://localhost:" + port + "/message/test?clientId=test&action=unsubscribe")
            .method(HttpMethod.POST).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                buf2.append(getContentAsString());
                latch2.countDown();
            }
        });

        latch2.await();
        httpClient.stop();

        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=localhost,Type=Subscription,destinationType=Queue,destinationName=test,*");
        Set<ObjectName> subs = broker.getManagementContext().queryNames(query, null);
        assertEquals("Consumers not closed", 0 , subs.size());
    }

    @Test(timeout = 15 * 1000)
    public void testPost() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        final AtomicInteger status = new AtomicInteger();
        httpClient.newRequest("http://localhost:" + port + "/message/testPost?type=queue")
           .method(HttpMethod.POST).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                status.getAndSet(result.getResponse().getStatus());
                buf.append(getContentAsString());
                latch.countDown();
            }
        });


        latch.await();
        assertTrue("success status", HttpStatus.isSuccess(status.get()));

        final StringBuffer buf2 = new StringBuffer();
        final AtomicInteger status2 = new AtomicInteger();
        final CountDownLatch latch2 =
                asyncRequest(httpClient, "http://localhost:" + port + "/message/testPost?readTimeout=1000&type=Queue", buf2, status2);

        latch2.await();
        assertTrue("success status", HttpStatus.isSuccess(status2.get()));
    }

    // test for https://issues.apache.org/activemq/browse/AMQ-3857
    @Test(timeout = 15 * 1000)
    public void testProperties() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        final AtomicInteger status = new AtomicInteger();
        httpClient.newRequest("http://localhost:" + port + "/message/testPost?type=queue&property=value")
           .method(HttpMethod.POST).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                status.getAndSet(result.getResponse().getStatus());
                buf.append(getContentAsString());
                latch.countDown();
            }
        });

        latch.await();
        assertTrue("success status", HttpStatus.isSuccess(status.get()));

        final CountDownLatch latch2 = new CountDownLatch(1);
        final StringBuffer buf2 = new StringBuffer();
        final AtomicInteger status2 = new AtomicInteger();
        final HttpFields responseFields = new HttpFields();
        httpClient.newRequest("http://localhost:" + port + "/message/testPost?readTimeout=1000&type=Queue")
           .method(HttpMethod.GET).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                responseFields.add(result.getResponse().getHeaders());
                status2.getAndSet(result.getResponse().getStatus());
                buf2.append(getContentAsString());
                latch2.countDown();
            }
        });

        latch2.await();
        assertTrue("success status", HttpStatus.isSuccess(status2.get()));

        HttpFields fields = responseFields;
        assertNotNull("Headers Exist", fields);
        assertEquals("header value", "value", fields.getStringField("property"));
    }


    @Test(timeout = 15 * 1000)
    public void testAuth() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();

        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        final AtomicInteger status = new AtomicInteger();
        httpClient.newRequest("http://localhost:" + port + "/message/testPost?type=queue")
            .header("Authorization", "Basic YWRtaW46YWRtaW4=")
           .method(HttpMethod.POST).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                status.getAndSet(result.getResponse().getStatus());
                buf.append(getContentAsString());
                latch.countDown();
            }
        });


        latch.await();
        assertTrue("success status", HttpStatus.isSuccess(status.get()));
    }

    protected CountDownLatch asyncRequest(final HttpClient httpClient, final String url, final StringBuffer buffer) {
        final CountDownLatch latch = new CountDownLatch(1);
        httpClient.newRequest(url).send(new BufferingResponseListener() {
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
