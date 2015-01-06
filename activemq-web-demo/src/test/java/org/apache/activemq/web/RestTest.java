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

import java.util.Set;

import javax.jms.TextMessage;
import javax.management.ObjectName;

import org.apache.commons.lang.RandomStringUtils;
import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class RestTest extends JettyTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(RestTest.class);

    @Test(timeout = 60 * 1000)
    public void testConsume() throws Exception {
        int port = getPort();

        producer.send(session.createTextMessage("test"));
        LOG.info("message sent");

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setURL("http://localhost:" + port + "/message/test?readTimeout=1000&type=queue");
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        assertEquals("test", contentExchange.getResponseContent());
    }

    @Test(timeout = 60 * 1000)
    public void testSubscribeFirst() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setURL("http://localhost:" + port + "/message/test?readTimeout=5000&type=queue");
        httpClient.send(contentExchange);

        Thread.sleep(1000);

        producer.send(session.createTextMessage("test"));
        LOG.info("message sent");

        contentExchange.waitForDone();
        assertEquals("test", contentExchange.getResponseContent());
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
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setURL("http://localhost:" + port + "/message/test?readTimeout=1000&type=queue");
        contentExchange.setRequestHeader("selector", "test=2");
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        assertEquals("test2", contentExchange.getResponseContent());
    }

    // test for https://issues.apache.org/activemq/browse/AMQ-2827
    @Test(timeout = 15 * 1000)
    public void testCorrelation() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        httpClient.start();

        for (int i = 0; i < 200; i++) {
            String correlId = "RESTY" + RandomStringUtils.randomNumeric(10);

            TextMessage message = session.createTextMessage(correlId);
            message.setStringProperty("correlationId", correlId);
            message.setJMSCorrelationID(correlId);

            LOG.info("Sending: " + correlId);
            producer.send(message);

            ContentExchange contentExchange = new ContentExchange();
            contentExchange.setURL("http://localhost:" + port + "/message/test?readTimeout=1000&type=queue&clientId=test");
            httpClient.send(contentExchange);
            contentExchange.waitForDone();
            LOG.info("Received: [" + contentExchange.getResponseStatus() + "] " + contentExchange.getResponseContent());
            assertEquals(200, contentExchange.getResponseStatus());
            assertEquals(correlId, contentExchange.getResponseContent());
        }
        httpClient.stop();
    }

    @Test(timeout = 15 * 1000)
    public void testDisconnect() throws Exception {
        int port = getPort();

        producer.send(session.createTextMessage("test"));
        HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setURL("http://localhost:" + port + "/message/test?readTimeout=1000&type=queue&clientId=test");
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        LOG.info("Received: [" + contentExchange.getResponseStatus() + "] " + contentExchange.getResponseContent());

        contentExchange = new ContentExchange();
        contentExchange.setMethod("POST");
        contentExchange.setURL("http://localhost:" + port + "/message/test?clientId=test&action=unsubscribe");
        httpClient.send(contentExchange);
        contentExchange.waitForDone();

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
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setMethod("POST");
        contentExchange.setURL("http://localhost:" + port + "/message/testPost?type=queue");
        httpClient.send(contentExchange);

        contentExchange.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange.getResponseStatus()));

        ContentExchange contentExchange2 = new ContentExchange();
        contentExchange2.setURL("http://localhost:" + port + "/message/testPost?readTimeout=1000&type=Queue");
        httpClient.send(contentExchange2);
        contentExchange2.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange2.getResponseStatus()));
    }

    // test for https://issues.apache.org/activemq/browse/AMQ-3857
    @Test(timeout = 15 * 1000)
    public void testProperties() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setMethod("POST");
        contentExchange.setURL("http://localhost:" + port + "/message/testPost?type=queue&property=value");
        httpClient.send(contentExchange);

        contentExchange.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange.getResponseStatus()));

        ContentExchange contentExchange2 = new ContentExchange(true);
        contentExchange2.setURL("http://localhost:" + port + "/message/testPost?readTimeout=1000&type=Queue");
        httpClient.send(contentExchange2);
        contentExchange2.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange2.getResponseStatus()));

        HttpFields fields = contentExchange2.getResponseFields();
        assertNotNull("Headers Exist", fields);
        assertEquals("header value", "value", fields.getStringField("property"));
    }


    @Test(timeout = 15 * 1000)
    public void testAuth() throws Exception {
        int port = getPort();

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        ContentExchange contentExchange = new ContentExchange();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
        contentExchange.setMethod("POST");
        contentExchange.setURL("http://localhost:" + port + "/message/testPost?type=queue");
        contentExchange.setRequestHeader("Authorization", "Basic YWRtaW46YWRtaW4=");
        httpClient.send(contentExchange);

        contentExchange.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange.getResponseStatus()));
    }
}
