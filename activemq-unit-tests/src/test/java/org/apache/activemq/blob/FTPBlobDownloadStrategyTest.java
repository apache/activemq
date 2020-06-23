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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;

public class FTPBlobDownloadStrategyTest extends FTPTestSupport {

    final int FILE_SIZE = Short.MAX_VALUE * 10;

    public void testDownload() throws Exception {
        setConnection();

        // create file
        File uploadFile = new File(ftpHomeDirFile, "test.txt");
        FileWriter wrt = new FileWriter(uploadFile);

        wrt.write("hello world");

        for(int ix = 0; ix < FILE_SIZE; ++ix ) {
            wrt.write("a");
        }

        wrt.close();

        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(ftpUrl);
        BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy(transferPolicy);
        InputStream stream;
        try {
            message.setURL(new URL(ftpUrl + "test.txt"));
            stream = strategy.getInputStream(message);
            int i = stream.read();
            StringBuilder sb = new StringBuilder(2048);
            while(i != -1) {
                sb.append((char)i);
                i = stream.read();
            }
            assertEquals("hello world", sb.toString().substring(0, "hello world".length()));
            assertEquals(FILE_SIZE, sb.toString().substring("hello world".length()).length());

            assertTrue(uploadFile.exists());
            strategy.deleteFile(message);
            assertFalse(uploadFile.exists());

        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }

    public void testWrongAuthentification() throws Exception {
        setConnection();

        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(ftpUrl);
        BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy(transferPolicy);
        try {
            message.setURL(new URL("ftp://" + userNamePass + "_wrong:" + userNamePass + "@localhost:"	+ ftpPort + "/ftptest/"));
            strategy.getInputStream(message);
        } catch(JMSException e) {
            assertEquals("Wrong Exception", "Cant Authentificate to FTP-Server", e.getMessage());
            return;
        } catch(Exception e) {
            System.out.println(e);
            assertTrue("Wrong Exception "+ e, false);
            return;
        }

        assertTrue("Expect Exception", false);
    }

    public void testWrongFTPPort() throws Exception {
        setConnection();

        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        BlobTransferPolicy transferPolicy = new BlobTransferPolicy();
        transferPolicy.setUploadUrl(ftpUrl);
        BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy(transferPolicy);
        try {
            message.setURL(new URL("ftp://" + userNamePass + ":" + userNamePass + "@localhost:"	+ 422 + "/ftptest/"));
            strategy.getInputStream(message);
        } catch (IOException e) {
            assertEquals("Wrong Exception", "The message URL port is incorrect", e.getMessage());
            return;
        }

        assertTrue("Expect Exception", false);
    }
}
