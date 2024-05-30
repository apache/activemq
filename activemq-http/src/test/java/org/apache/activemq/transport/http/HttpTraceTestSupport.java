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
package org.apache.activemq.transport.http;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.Request;
import org.eclipse.jetty.client.Result;
import org.eclipse.jetty.client.transport.HttpClientTransportDynamic;
import org.eclipse.jetty.client.BufferingResponseListener;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.io.ClientConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class HttpTraceTestSupport {

    public static List<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][] {
            //value is empty
            {"http.enableTrace=", HttpStatus.FORBIDDEN_403},
            //default, trace method not specified
            {null, HttpStatus.FORBIDDEN_403},
            // enable http trace method
            {"http.enableTrace=true", HttpStatus.OK_200},
            // disable trace method
            {"http.enableTrace=false", HttpStatus.FORBIDDEN_403}
        });
    }

    public static void testHttpTraceEnabled(final String uri, final int expectedStatus) throws Exception {
        testHttpTraceEnabled(uri, expectedStatus, new SslContextFactory.Client());
    }

    public static void testHttpTraceEnabled(final String uri, final int expectedStatus, SslContextFactory.Client
            sslContextFactory) throws Exception {

        ClientConnector clientConnector = new ClientConnector();
        clientConnector.setSslContextFactory(sslContextFactory);

        HttpClient httpClient = sslContextFactory != null ? new HttpClient(new HttpClientTransportDynamic(clientConnector)) : new HttpClient();
        httpClient.start();

        final CountDownLatch latch = new CountDownLatch(1);
        Request request = httpClient.newRequest(uri).method(HttpMethod.TRACE);
        final AtomicInteger status = new AtomicInteger();
        request.send(new BufferingResponseListener() {
            @Override
            public void onComplete(Result result) {
                status.set(result.getResponse().getStatus());
                latch.countDown();
            }
        });
        latch.await();
        assertEquals(expectedStatus, status.get());
    }
}
