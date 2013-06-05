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

import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQBlobMessage;


public class FTPBlobUploadStrategyTest extends FTPTestSupport {

    public void testFileUpload() throws Exception {
        setConnection();
        File file = File.createTempFile("amq-data-file-", ".dat");
        // lets write some data
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.append("hello world");
        writer.close();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ((ActiveMQConnection)connection).setCopyMessageOnSend(false);

        ActiveMQBlobMessage message = (ActiveMQBlobMessage) ((ActiveMQSession)session).createBlobMessage(file);
        message.setJMSMessageID("testmessage");
        message.onSend();
        assertEquals(ftpUrl + "ID_testmessage", message.getURL().toString());
        File uploaded = new File(ftpHomeDirFile, "ID_testmessage");
        assertTrue("File doesn't exists", uploaded.exists());
    }

    public void testWriteDenied() throws Exception {
        userNamePass = "guest";
        setConnection();
        File file = File.createTempFile("amq-data-file-", ".dat");
        // lets write some data
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.append("hello world");
        writer.close();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ((ActiveMQConnection)connection).setCopyMessageOnSend(false);

        ActiveMQBlobMessage message = (ActiveMQBlobMessage) ((ActiveMQSession)session).createBlobMessage(file);
        message.setJMSMessageID("testmessage");
        try {
            message.onSend();
        } catch (JMSException e) {
            e.printStackTrace();
            return;
        }
        fail("Should have failed with permission denied exception!");
    }

}
