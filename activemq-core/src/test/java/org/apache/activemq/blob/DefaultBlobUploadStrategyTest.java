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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URL;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.MessageId;

public class DefaultBlobUploadStrategyTest extends TestCase {

    private static final String FILESERVER_URL = "http://localhost:8080/";
    private static final String URI = "vm://localhost?jms.blobTransferPolicy.defaultUploadUrl=http://localhost:8080/";

    public static void main(String[] args) {
        junit.textui.TestRunner.run(DefaultBlobUploadStrategyTest.class);
    }

    public void testDummy() throws Exception {

    }

    // DISABLED UNTIL WE EMBED JETTY
    public void xtestUploadViaDefaultBlobUploadStrategy() throws Exception {
        // 0. Initialise
        File file = File.createTempFile("amq-data-file-", ".dat");
        // lets write some data
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.append("Hello World!");
        writer.close();

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(URI);
        BlobTransferPolicy policy = factory.getBlobTransferPolicy();

        ActiveMQBlobMessage msg = new ActiveMQBlobMessage();
        msg.setMessageId(new MessageId());

        // 1. Upload
        DefaultBlobUploadStrategy strategy = new DefaultBlobUploadStrategy(policy);
        strategy.uploadFile(msg, file);

        // 2. Download
        msg.setURL(new URL(FILESERVER_URL + msg.getMessageId()));

        InputStream in = msg.getInputStream();
        long bytesRead = 0;
        byte[] buffer = new byte[1024 * 1024];

        while (true) {
            int c = in.read(buffer);
            if (c == -1) {
                break;
            }
            bytesRead += c;
        }
        in.close();
        TestCase.assertTrue(bytesRead == file.length());

        // 3. Delete
        //strategy.deleteFile(msg);
    }

}
