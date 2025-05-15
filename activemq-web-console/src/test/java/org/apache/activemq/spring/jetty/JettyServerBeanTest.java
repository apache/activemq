/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.spring.jetty;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.Test;

public class JettyServerBeanTest {

    @Test
    public void testJettyServerBean() throws Exception {
        var context = new ClassPathXmlApplicationContext("conf/jetty-spring.xml");
        context.start();

        var jettyServer = (JettyServerBean) context.getBean("jettyServer");
        assertNotNull(jettyServer);

        Server server = jettyServer.getServer();
        assertNotNull("Embedded Jetty Server should be created", server);
        assertTrue("Embedded Jetty Server should be running", server.isRunning());

        // The console is protected by the JAAS SecurityHandler wired in
        // jetty-security.xml: an unauthenticated request must be challenged
        // (HTTP 401) rather than served. A request from loopback also confirms
        // the InetAccessHandler allows local clients.
        int port = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
        HttpClient client = HttpClient.newHttpClient();
        HttpResponse<String> response = client.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/admin/")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals("Web console must require authentication", 401, response.statusCode());

        context.stop();
    }
}
