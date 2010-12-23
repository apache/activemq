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


import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.jms.MessageProducer;
import javax.jms.TextMessage;

public class AjaxTest extends JettyTestSupport {
    private static final Log LOG = LogFactory.getLog(AjaxTest.class);

    private String expectedResponse = "<ajax-response>\n" +
            "<response id='handler' destination='queue://test' >test one</response>\n" +
            "<response id='handler' destination='queue://test' >test two</response>\n" +
            "<response id='handler' destination='queue://test' >test three</response>\n" +
            "</ajax-response>";

    public void testReceiveMultipleMessagesFromQueue() throws Exception {

        MessageProducer local_producer = session.createProducer(session.createQueue("test"));

        HttpClient httpClient = new HttpClient();
        PostMethod post = new PostMethod( "http://localhost:8080/amq" );
        post.addParameter( "destination", "queue://test" );
        post.addParameter( "type", "listen" );
        post.addParameter( "message", "handler" );
        httpClient.executeMethod( post );

        // send message
        TextMessage msg1 = session.createTextMessage("test one");
        producer.send(msg1);
        TextMessage msg2 = session.createTextMessage("test two");
        producer.send(msg2);
        TextMessage msg3 = session.createTextMessage("test three");
        producer.send(msg3);

        HttpMethod get = new GetMethod( "http://localhost:8080/amq?timeout=5000" );
        httpClient.executeMethod( get );
        byte[] responseBody = get.getResponseBody();
        String response = new String( responseBody );

        LOG.info("Poll response: " + response);
        assertEquals("Poll response not right", expectedResponse.trim(), response.trim());
    }

}
