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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.junit.Test;

public class DownloadStrategyTest {
    
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
    
    @Test
    public void testFTPBlobDownloadStrategy() throws Exception {
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl("ftp://localhost:22");
        BlobDownloadStrategy downloadStrategy = new FTPBlobDownloadStrategy(transferPolicy);
        
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

        // Test host
        message.setURL(new URL("ftp://some-ip:22/somedoc"));
        try {
            downloadStrategy.getInputStream(message);
            fail("Failure expected on an incorrect blob message URL");
        } catch (IOException ex) {
            // expected
            assertEquals("The message URL host is incorrect", ex.getMessage());
        }
        
        // Test port
        message.setURL(new URL("ftp://localhost:12345/somedoc"));
        try {
            downloadStrategy.getInputStream(message);
            fail("Failure expected on an incorrect blob message URL");
        } catch (IOException ex) {
            // expected
            assertEquals("The message URL port is incorrect", ex.getMessage());
        }
        
        // This is OK (but won't connect)
        message.setURL(new URL("ftp://localhost:22/somedoc"));
        try {
            downloadStrategy.getInputStream(message);
            fail("Failure expected on connection");
        } catch (IOException | JMSException ex) {
            // expected
        }
    }
}