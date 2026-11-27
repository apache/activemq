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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import jakarta.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.MessageId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HttpBlobTransferTest {

    private Path root;
    private LocalFileBlobServer blobServer;
    private BlobTransferPolicy policy;

    @Before
    public void setUp() throws Exception {
        root = Files.createTempDirectory("http-blob-test");
        blobServer = new LocalFileBlobServer(root);
        blobServer.start();
        policy = new BlobTransferPolicy();
        policy.setUploadUrl(blobServer.getUploadUrl());
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "http");
    }

    @After
    public void tearDown() throws Exception {
        System.clearProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY);
        if (blobServer != null) {
            blobServer.stop();
        }
        if (root != null) {
            try (var paths = Files.walk(root)) {
                paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    private ActiveMQBlobMessage blobMessage(String messageId) {
        var message = new ActiveMQBlobMessage();
        message.setMessageId(new MessageId(messageId));
        return message;
    }

    @Test
    public void testUploadDownloadDeleteRoundTrip() throws Exception {
        var message = blobMessage("ID:producer:1:1:1");
        var content = "large out-of-band payload".getBytes(StandardCharsets.UTF_8);

        var url = new DefaultBlobUploadStrategy(policy)
                .uploadStream(message, new ByteArrayInputStream(content));
        message.setURL(url);
        assertTrue("blob should be stored on the server", blobServer.exists("ID:producer:1:1:1"));

        var download = new DefaultBlobDownloadStrategy(policy);
        try (var in = download.getInputStream(message)) {
            assertEquals(new String(content, StandardCharsets.UTF_8),
                    new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        download.deleteFile(message);
        assertFalse("blob should be deleted from the server", blobServer.exists("ID:producer:1:1:1"));
    }

    @Test
    public void testDeleteFollowsStoredUrlNotReconstructed() throws Exception {
        blobServer.put("server/chosen/location", "payload".getBytes(StandardCharsets.UTF_8));
        assertTrue(blobServer.exists("server/chosen/location"));

        var message = blobMessage("ID:producer:9:9:9");
        message.setURL(new URL(blobServer.getUploadUrl() + "server/chosen/location"));

        new DefaultBlobDownloadStrategy(policy).deleteFile(message);

        assertFalse("delete must follow the stored URL", blobServer.exists("server/chosen/location"));
    }

    @Test
    public void testDeleteFileWithoutDownloaderIsNoOp() throws Exception {
        var message = blobMessage("ID:producer:2:2:2");
        message.deleteFile();
    }

    @Test
    public void testDeleteFileViaMessageWithDownloader() throws Exception {
        var message = blobMessage("ID:producer:3:3:3");
        var url = new DefaultBlobUploadStrategy(policy)
                .uploadStream(message, new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)));
        message.setURL(url);
        message.setBlobDownloader(new BlobDownloader(policy));
        assertTrue(blobServer.exists("ID:producer:3:3:3"));

        message.deleteFile();

        assertFalse(blobServer.exists("ID:producer:3:3:3"));
    }

    @Test
    public void testSetDownloadStrategy() {
        var custom = new DefaultBlobDownloadStrategy(policy);
        policy.setDownloadStrategy(custom);
        assertSame("explicitly set download strategy should be returned",
                custom, policy.getDownloadStrategy());
    }

    @Test
    public void testDeleteRejectsForeignHost() throws Exception {
        var message = blobMessage("ID:producer:4:4:4");
        message.setURL(new URL("http://evil.example.com:" + hostPort() + "/uploads/ID:producer:4:4:4"));
        try {
            new DefaultBlobDownloadStrategy(policy).deleteFile(message);
            fail("expected the foreign host to be rejected");
        } catch (IOException expected) {
            assertEquals("The message URL host is incorrect", expected.getMessage());
        }
    }

    private int hostPort() throws Exception {
        return new URL(blobServer.getUploadUrl()).getPort();
    }
}
