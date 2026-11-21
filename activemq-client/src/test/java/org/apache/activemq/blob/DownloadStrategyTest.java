/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Set;

import jakarta.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.MessageId;
import org.junit.After;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DownloadStrategyTest {

    private static final String NOT_ALLOWED = "' is not allowed for blob transfer."
        + " Allowed protocols are set with " + BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY;
    private static final String FTP_NOT_ALLOWED = "The URL protocol 'ftp" + NOT_ALLOWED;
    private static final String FTP_NEEDS_HTTP =
        "The default blob strategy needs an http or https URL to upload or delete but the protocol is 'ftp'";

    @Rule
    public TemporaryFolder blobFolder = new TemporaryFolder();

    @After
    public void clearAllowedProtocols() {
        System.clearProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY);
    }

    @Test
    public void testDefaultUploadUrl() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        assertEquals("https://localhost:8080/uploads/", transferPolicy.getDefaultUploadUrl());
        assertEquals("https://localhost:8080/uploads/", transferPolicy.getUploadUrl());
    }

    @Test
    public void testDefaultAllowedProtocols() throws Exception {
        assertEquals(Set.of("file", "https"), BlobTransferPolicy.DEFAULT_ALLOWED_PROTOCOLS);
        assertEquals(BlobTransferPolicy.DEFAULT_ALLOWED_PROTOCOLS, BlobTransferPolicy.getAllowedProtocols());

        assertNotAllowed("http", () -> upload("http://localhost/blobs/"));
        assertNotAllowed("http", () -> download("http://localhost/blobs/"));
        assertNotAllowed("http", () -> delete("http://localhost/blobs/"));
    }

    @Test
    public void testAllowedProtocolsProperty() throws Exception {
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "https");
        assertEquals(Set.of("https"), BlobTransferPolicy.getAllowedProtocols());

        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, " HTTPS , http ");
        assertEquals(Set.of("http", "https"), BlobTransferPolicy.getAllowedProtocols());

        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "");
        assertEquals(Set.of(), BlobTransferPolicy.getAllowedProtocols());
    }

    @Test
    public void testAllowedProtocolsPropertyCanAddFtp() throws Exception {
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "ftp,https");
        assertEquals(Set.of("ftp", "https"), BlobTransferPolicy.getAllowedProtocols());
        BlobTransferPolicy.checkProtocolAllowed("ftp");

        // nothing listens on port 1
        IOException downloadFailure = assertThrows(IOException.class, () -> download("ftp://localhost:1/blobs/"));
        assertFalse(downloadFailure.toString(), downloadFailure.toString().contains("not allowed"));

        IOException uploadFailure = assertThrows(IOException.class, () -> upload("ftp://localhost/blobs/"));
        assertEquals(FTP_NEEDS_HTTP, uploadFailure.getMessage());
        IOException deleteFailure = assertThrows(IOException.class, () -> delete("ftp://localhost/blobs/"));
        assertEquals(FTP_NEEDS_HTTP, deleteFailure.getMessage());
    }

    @Test
    public void testOnlyHttpsAllowed() throws Exception {
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "https");

        BlobTransferPolicy.checkProtocolAllowed("https");
        assertNotAllowed("http", () -> upload("http://localhost/blobs/"));
        assertNotAllowed("http", () -> download("http://localhost/blobs/"));
        assertNotAllowed("http", () -> delete("http://localhost/blobs/"));
        assertNotAllowed("file", () -> upload(blobFolder.getRoot().toURI().toString()));
    }

    @Test
    public void testNoProtocolAllowed() throws Exception {
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "");

        assertNotAllowed("https", () -> upload("https://localhost/blobs/"));
        assertNotAllowed("https", () -> download("https://localhost/blobs/"));
        assertNotAllowed("http", () -> upload("http://localhost/blobs/"));
        assertNotAllowed("file", () -> upload(blobFolder.getRoot().toURI().toString()));
        assertNotAllowed("file", () -> download(blobFolder.getRoot().toURI().toString()));
        assertNotAllowed("file", () -> delete(blobFolder.getRoot().toURI().toString()));
    }

    @Test
    public void testFileAllowedWhenListed() throws Exception {
        System.setProperty(BlobTransferPolicy.ALLOWED_PROTOCOLS_PROPERTY, "file");

        final String uploadUrl = blobFolder.getRoot().toURI().toString();
        upload(uploadUrl);
        download(uploadUrl).close();
        delete(uploadUrl);
        assertNotAllowed("https", () -> upload("https://localhost/blobs/"));
    }

    private static final String PATH_INCORRECT = "The message URL path is incorrect";

    private static DefaultBlobDownloadStrategy defaultStrategy(String uploadUrl) {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadUrl);
        return new DefaultBlobDownloadStrategy(transferPolicy);
    }

    private static void assertPathIncorrect(DefaultBlobDownloadStrategy strategy, String url) throws Exception {
        final URL messageUrl = new URL(url);
        IOException ex = assertThrows(url, IOException.class, () -> strategy.validateMessageURL(messageUrl));
        assertEquals(url, PATH_INCORRECT, ex.getMessage());
    }

    @Test
    public void testMessageUrlMustBeUnderUploadUrlPath() throws Exception {
        final DefaultBlobDownloadStrategy strategy = defaultStrategy("https://blob-server/uploads/");

        strategy.validateMessageURL(new URL("https://blob-server/uploads/ID:producer:1:1:1"));
        strategy.validateMessageURL(new URL("https://blob-server/uploads/server/chosen/location"));
        strategy.validateMessageURL(new URL("https://blob-server/uploads/file?version=2"));

        assertPathIncorrect(strategy, "https://blob-server/other");
        assertPathIncorrect(strategy, "https://blob-server/");
        assertPathIncorrect(strategy, "https://blob-server/uploads-other/file");
        assertPathIncorrect(strategy, "https://blob-server/uploads/../other");
        assertPathIncorrect(strategy, "https://blob-server/uploads/%2e%2e/other");
        assertPathIncorrect(strategy, "https://blob-server/uploads/sub/..%2f..%2fother");
        assertPathIncorrect(strategy, "https://blob-server/uploads//../other");
        assertPathIncorrect(strategy, "https://blob-server/uploads/sub/../file");
        assertPathIncorrect(strategy, "https://blob-server/uploads/..\\other");
    }

    @Test
    public void testMessageUrlPathWithoutTrailingSlashOnUploadUrl() throws Exception {
        final DefaultBlobDownloadStrategy strategy = defaultStrategy("https://blob-server/uploads");

        strategy.validateMessageURL(new URL("https://blob-server/uploads/file"));
        // the upload appends the message id straight onto the upload URL
        strategy.validateMessageURL(new URL("https://blob-server/uploadsID:producer:1:1:1"));

        assertPathIncorrect(strategy, "https://blob-server/other");
        assertPathIncorrect(strategy, "https://blob-server/uploads/../other");
    }

    @Test
    public void testMessageUrlPathWithNoPathOnUploadUrl() throws Exception {
        final DefaultBlobDownloadStrategy strategy = defaultStrategy("https://blob-server");

        strategy.validateMessageURL(new URL("https://blob-server/anything/at/all"));
        assertPathIncorrect(strategy, "https://blob-server/anything/../at/all");
    }

    @Test
    public void testDownloadAndDeleteRefuseUrlOutsideUploadUrlPath() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setBrokerUploadUrl("https://blob-server/uploads/");
        final BlobDownloadStrategy strategy = transferPolicy.getDownloadStrategy();

        final ActiveMQBlobMessage message = blobMessage();
        message.setURL(new URL("https://blob-server/other"));

        IOException read = assertThrows(IOException.class, () -> strategy.getInputStream(message));
        assertEquals(PATH_INCORRECT, read.getMessage());
        IOException delete = assertThrows(IOException.class, () -> strategy.deleteFile(message));
        assertEquals(PATH_INCORRECT, delete.getMessage());
    }

    private static final String OUTSIDE_UPLOAD_PATH = "The message URL path is outside the upload URL path";

    private static ActiveMQBlobMessage fileMessage(String url) throws Exception {
        ActiveMQBlobMessage message = blobMessage();
        message.setURL(new URL(url));
        return message;
    }

    private void assertOutsideUploadPath(BlobDownloadStrategy strategy, String url) throws Exception {
        final ActiveMQBlobMessage message = fileMessage(url);
        IOException read = assertThrows(url, IOException.class, () -> strategy.getInputStream(message));
        assertEquals(OUTSIDE_UPLOAD_PATH, read.getMessage());
        IOException delete = assertThrows(url, IOException.class, () -> strategy.deleteFile(message));
        assertEquals(OUTSIDE_UPLOAD_PATH, delete.getMessage());
    }

    @Test
    public void testFileStrategyRefusesFileOutsideUploadFolder() throws Exception {
        final File uploadFolder = blobFolder.newFolder("blobs");
        final File outside = blobFolder.newFile("outside.txt");
        Files.writeString(outside.toPath(), "secret");
        blobFolder.newFolder("blobs-other");
        final File sibling = new File(blobFolder.getRoot(), "blobs-other/file.txt");
        Files.writeString(sibling.toPath(), "secret");

        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadFolder.toURI().toString());
        BlobDownloadStrategy strategy = transferPolicy.getDownloadStrategy();

        final String uploadUrl = uploadFolder.toURI().toString();
        assertOutsideUploadPath(strategy, outside.toURI().toString());
        assertOutsideUploadPath(strategy, uploadUrl + "../outside.txt");
        assertOutsideUploadPath(strategy, uploadUrl + "%2e%2e/outside.txt");
        assertOutsideUploadPath(strategy, uploadUrl + "sub/../../outside.txt");
        assertOutsideUploadPath(strategy, sibling.toURI().toString());

        assertTrue("nothing outside the upload folder was deleted", outside.exists() && sibling.exists());
    }

    @Test
    public void testFileStrategyRefusesFileOutsideBrokerUploadFolder() throws Exception {
        final File uploadFolder = blobFolder.newFolder("blobs");
        final File outside = blobFolder.newFile("outside.txt");

        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setBrokerUploadUrl(uploadFolder.toURI().toString());

        assertOutsideUploadPath(transferPolicy.getDownloadStrategy(), outside.toURI().toString());
        assertTrue(outside.exists());
    }

    @Test
    public void testFileStrategyRefusesMessageIdThatLeavesUploadFolder() throws Exception {
        final File uploadFolder = blobFolder.newFolder("blobs");
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadFolder.toURI().toString());
        BlobDownloadStrategy strategy = transferPolicy.getDownloadStrategy();

        // no URL on the message, so the file name comes from the message id
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setMessageId(new MessageId("ID:x/../../outside:1:1:1"));

        IOException ex = assertThrows(IOException.class, () -> strategy.deleteFile(message));
        assertEquals(OUTSIDE_UPLOAD_PATH, ex.getMessage());
    }

    @Test
    public void testFileStrategyReadsAndDeletesUnderUploadFolder() throws Exception {
        final File uploadFolder = blobFolder.newFolder("blobs");
        final File nested = new File(uploadFolder, "sub/inside.txt");
        new File(uploadFolder, "sub/other").mkdirs();
        Files.writeString(nested.toPath(), "blob");

        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadFolder.toURI().toString());
        BlobDownloadStrategy strategy = transferPolicy.getDownloadStrategy();

        final ActiveMQBlobMessage message = fileMessage(uploadFolder.toURI() + "sub/other/../inside.txt");
        try (var in = strategy.getInputStream(message)) {
            assertEquals("blob", new String(in.readAllBytes()));
        }
        strategy.deleteFile(message);
        assertFalse(nested.exists());
    }

    @Test
    public void testFileStrategyFollowsSymbolicLinkInUploadFolder() throws Exception {
        final File uploadFolder = blobFolder.newFolder("blobs");
        final File outside = blobFolder.newFile("outside.txt");
        Files.writeString(outside.toPath(), "linked");
        final File link = new File(uploadFolder, "link.txt");
        try {
            Files.createSymbolicLink(link.toPath(), outside.toPath());
        } catch (IOException | UnsupportedOperationException e) {
            Assume.assumeNoException("symbolic links are not available here", e);
        }

        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadFolder.toURI().toString());

        try (var in = transferPolicy.getDownloadStrategy().getInputStream(fileMessage(link.toURI().toString()))) {
            assertEquals("linked", new String(in.readAllBytes()));
        }
    }

    private interface Transfer {
        void run() throws Exception;
    }

    private static void assertNotAllowed(String protocol, Transfer transfer) {
        IOException ex = assertThrows(IOException.class, transfer::run);
        assertEquals("The URL protocol '" + protocol + NOT_ALLOWED, ex.getMessage());
    }

    private static ActiveMQBlobMessage blobMessage() {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setMessageId(new MessageId("ID:test-1:1:1:1"));
        return message;
    }

    private static void upload(String uploadUrl) throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadUrl);
        transferPolicy.getUploadStrategy().uploadStream(blobMessage(), new ByteArrayInputStream(new byte[] {1}));
    }

    private static java.io.InputStream download(String uploadUrl) throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadUrl);
        ActiveMQBlobMessage message = blobMessage();
        if (!uploadUrl.startsWith("file:")) {
            message.setURL(new URL(uploadUrl + "file"));
        }
        return transferPolicy.getDownloadStrategy().getInputStream(message);
    }

    private static void delete(String uploadUrl) throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(uploadUrl);
        ActiveMQBlobMessage message = blobMessage();
        if (!uploadUrl.startsWith("file:")) {
            message.setURL(new URL(uploadUrl + "file"));
        }
        transferPolicy.getDownloadStrategy().deleteFile(message);
    }

    @Test
    public void testDefaultBlobDownloadStrategyRejectsFtp() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl("ftp://localhost/blobs/");
        BlobDownloadStrategy downloadStrategy = transferPolicy.getDownloadStrategy();

        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setURL(new URL("ftp://localhost/blobs/file"));

        IOException ex = assertThrows(IOException.class, () -> downloadStrategy.getInputStream(message));
        assertEquals(FTP_NOT_ALLOWED, ex.getMessage());
    }

    @Test
    public void testDefaultBlobDownloadStrategyRejectsFtpDelete() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl("ftp://localhost/blobs/");
        BlobDownloadStrategy downloadStrategy = transferPolicy.getDownloadStrategy();

        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setMessageId(new MessageId("ID:test-1:1:1:1"));
        message.setURL(new URL("ftp://localhost/blobs/file"));

        IOException ex = assertThrows(IOException.class, () -> downloadStrategy.deleteFile(message));
        assertEquals(FTP_NOT_ALLOWED, ex.getMessage());
    }

    @Test
    public void testDefaultBlobUploadStrategyRejectsFtp() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl("ftp://localhost/blobs/");
        BlobUploadStrategy uploadStrategy = transferPolicy.getUploadStrategy();

        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setMessageId(new MessageId("ID:test-1:1:1:1"));

        IOException ex = assertThrows(IOException.class,
            () -> uploadStrategy.uploadStream(message, new ByteArrayInputStream(new byte[] {1})));
        assertEquals(FTP_NOT_ALLOWED, ex.getMessage());
    }

    @Test
    public void testDefaultBlobDownloadStrategy() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        BlobDownloadStrategy downloadStrategy = new DefaultBlobDownloadStrategy(transferPolicy);
        
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        message.setURL(new URL("https://www.apache.org"));
        
        try {
            downloadStrategy.getInputStream(message);
            fail("Failure expected on an incorrect blob message URL");
        } catch (IOException ex) {
            // expected
        }
        
        // Now allow it
        transferPolicy.setUploadUrl("https://www.apache.org");
        downloadStrategy.getInputStream(message).close();
    }
    
    @Test
    public void testFileBlobDownloadStrategy() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl("file:/tmp/xyz");
        BlobDownloadStrategy downloadStrategy = new FileSystemBlobStrategy(transferPolicy);
        
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        
        // Test protocol
        message.setURL(new URL("https://www.apache.org"));
        try {
            downloadStrategy.getInputStream(message);
            fail("Failure expected on an incorrect blob message URL");
        } catch (IOException ex) {
            // expected
            assertEquals("The message URL protocol is incorrect", ex.getMessage());
        }   
    }
    
}
