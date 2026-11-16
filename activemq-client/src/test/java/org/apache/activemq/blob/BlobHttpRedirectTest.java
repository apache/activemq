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
package org.apache.activemq.blob;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.MessageId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

public class BlobHttpRedirectTest {

    private static final String TARGET = "/elsewhere/target";

    private final List<String> requests = new CopyOnWriteArrayList<>();
    private HttpServer server;
    private BlobTransferPolicy transferPolicy;

    @Before
    public void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/uploads/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            exchange.getRequestBody().readAllBytes();
            exchange.getResponseHeaders().add("Location", TARGET);
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.createContext(TARGET, exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI().getPath());
            exchange.getRequestBody().readAllBytes();
            final byte[] body = "moved".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();

        transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl("http://localhost:" + server.getAddress().getPort() + "/uploads/");
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "http");
    }

    @After
    public void tearDown() {
        System.clearProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY);
        System.clearProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY);
        if (server != null) {
            server.stop(0);
        }
    }

    private ActiveMQBlobMessage message() throws Exception {
        final ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setMessageId(new MessageId("ID:test-1:1:1:1"));
        message.setURL(new URL(transferPolicy.getUploadUrl() + "blob"));
        return message;
    }

    private boolean targetReached() {
        return requests.stream().anyMatch(request -> request.endsWith(" " + TARGET));
    }

    @Test
    public void testDownloadFollowsRedirectByDefault() throws Exception {
        assertTrue(BlobTransferPolicy.isHttpFollowRedirects());

        try (var in = transferPolicy.getDownloadStrategy().getInputStream(message())) {
            assertEquals("moved", new String(in.readAllBytes()));
        }
        assertTrue(requests.toString(), targetReached());
    }

    @Test
    public void testDownloadRefusesRedirectWhenNotFollowing() throws Exception {
        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "false");
        final ActiveMQBlobMessage message = message();

        IOException ex = assertThrows(IOException.class,
            () -> transferPolicy.getDownloadStrategy().getInputStream(message));
        assertTrue(ex.getMessage(), ex.getMessage().contains("was redirected (302)"));
        assertTrue(ex.getMessage(), ex.getMessage().contains(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY));
        assertEquals(List.of("GET /uploads/blob"), requests);
    }

    @Test
    public void testDeleteRefusesRedirectWhenNotFollowing() throws Exception {
        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "false");
        final ActiveMQBlobMessage message = message();

        IOException ex = assertThrows(IOException.class,
            () -> transferPolicy.getDownloadStrategy().deleteFile(message));
        assertTrue(ex.getMessage(), ex.getMessage().contains("302"));
        assertEquals(List.of("DELETE /uploads/blob"), requests);
    }

    @Test
    public void testUploadRefusesRedirectWhenNotFollowing() throws Exception {
        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "false");
        final ActiveMQBlobMessage message = message();

        IOException ex = assertThrows(IOException.class,
            () -> transferPolicy.getUploadStrategy().uploadStream(message, new ByteArrayInputStream(new byte[] {1})));
        assertTrue(ex.getMessage(), ex.getMessage().contains("302"));
        assertFalse(requests.toString(), targetReached());
    }

    @Test
    public void testFollowRedirectsPropertyValues() throws Exception {
        assertTrue("followed when not set", BlobTransferPolicy.isHttpFollowRedirects());

        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "true");
        assertTrue(BlobTransferPolicy.isHttpFollowRedirects());
        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, " TRUE ");
        assertTrue(BlobTransferPolicy.isHttpFollowRedirects());

        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "false");
        assertFalse(BlobTransferPolicy.isHttpFollowRedirects());
        // anything that is not true turns redirects off
        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "");
        assertFalse(BlobTransferPolicy.isHttpFollowRedirects());
        System.setProperty(BlobTransferPolicy.HTTP_FOLLOW_REDIRECTS_PROPERTY, "no");
        assertFalse(BlobTransferPolicy.isHttpFollowRedirects());
    }
}
