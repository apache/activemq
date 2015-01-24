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

import java.io.ByteArrayInputStream;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

public class RestPersistentTest extends JettyTestSupport {

    @Override
    protected boolean isPersistent() {
        // need persistent for post/get
        return true;
    }

    @Test(timeout = 60 * 1000)
    public void testPostAndGetWithQueue() throws Exception {
        postAndGet("queue");
    }

    @Test(timeout = 60 * 1000)
    @Ignore("Needs a JIRA")
    public void testPostAndGetWithTopic() throws Exception {
        // TODO: problems with topics
        // postAndGet("topic");
    }

    public void postAndGet(String destinationType) throws Exception {
        int port = getPort();

        final String urlGET="http://localhost:" + port + "/message/upcTest?clientId=consumer1&readTimeout=5000&type="+destinationType;
        final String urlPOST="http://localhost:" + port + "/message/upcTest?type="+destinationType;

        final String message1="<itemPolicy><upc>1001</upc></itemPolicy>";
        final String property1="terminalNumber=lane1";
        final String selector1="terminalNumber='lane1'";

        HttpClient httpClient = new HttpClient();
        httpClient.start();
        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);

        //post first message
        // TODO: a problem with GET before POST
        // getMessage(httpClient, urlGET, selector1, null);  //should NOT receive message1
        postMessage(httpClient, urlPOST, property1, message1);
        getMessage(httpClient, urlGET, selector1, message1);  //should receive message1
    }

    private void postMessage(HttpClient httpClient, String url, String properties, String message) throws Exception
    {
        ContentExchange contentExchange = new ContentExchange();
        contentExchange.setMethod("POST");
        contentExchange.setURL(url+"&"+properties);
        //contentExchange.setRequestHeader("accept", "text/xml");
        contentExchange.setRequestHeader("Content-Type","text/xml");
        contentExchange.setRequestContentSource(new ByteArrayInputStream(message.getBytes("UTF-8")));

        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange.getResponseStatus()));
     }

    private void getMessage(HttpClient httpClient, String url, String selector, String expectedMessage) throws Exception
    {
        ContentExchange contentExchange = new ContentExchange(true);
        contentExchange.setURL(url);
        contentExchange.setRequestHeader("accept", "text/xml");
        contentExchange.setRequestHeader("Content-Type","text/xml");
        if(selector!=null)
        {
            contentExchange.setRequestHeader("selector", selector);
        }
        httpClient.send(contentExchange);
        contentExchange.waitForDone();
        assertTrue("success status", HttpStatus.isSuccess(contentExchange.getResponseStatus()));

        if(expectedMessage!=null)
        {
            assertNotNull(contentExchange.getResponseContent());
            assertEquals(expectedMessage, contentExchange.getResponseContent().trim());
        }
     }
}
