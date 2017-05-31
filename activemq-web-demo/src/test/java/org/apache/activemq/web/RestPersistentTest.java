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

import java.io.ByteArrayInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BufferingResponseListener;
import org.eclipse.jetty.client.util.InputStreamContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.Ignore;
import org.junit.Test;

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

        //post first message
        // TODO: a problem with GET before POST
        // getMessage(httpClient, urlGET, selector1, null);  //should NOT receive message1
        postMessage(httpClient, urlPOST, property1, message1);
        getMessage(httpClient, urlGET, selector1, message1);  //should receive message1
    }

    private void postMessage(HttpClient httpClient, String url, String properties, String message) throws Exception
    {
        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        final AtomicInteger status = new AtomicInteger();
        httpClient.newRequest(url+"&"+properties)
            .header("Content-Type","text/xml")
           .content(new InputStreamContentProvider(new ByteArrayInputStream(message.getBytes("UTF-8"))))
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

    private void getMessage(HttpClient httpClient, String url, String selector, String expectedMessage) throws Exception
    {

        final CountDownLatch latch = new CountDownLatch(1);
        final StringBuffer buf = new StringBuffer();
        final AtomicInteger status = new AtomicInteger();
        Request request = httpClient.newRequest(url)
            .header("accept", "text/xml")
            .header("Content-Type","text/xml");

        if(selector!=null)
        {
            request.header("selector", selector);
        }

        request.method(HttpMethod.GET).send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                status.getAndSet(result.getResponse().getStatus());
                buf.append(getContentAsString());
                latch.countDown();
            }
        });

        latch.await();
        assertTrue("success status", HttpStatus.isSuccess(status.get()));

        if(expectedMessage!=null)
        {
            assertNotNull(buf.toString());
            assertEquals(expectedMessage, buf.toString().trim());
        }
     }
}
