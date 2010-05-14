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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import org.apache.activemq.command.ActiveMQBlobMessage;


public class FTPBlobTest extends FTPTestSupport {

    public void testBlobFile() throws Exception {
        setConnection();
        // first create Message
        File file = File.createTempFile("amq-data-file-", ".dat");
        // lets write some data
        String content = "hello world " + System.currentTimeMillis();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.append(content);
        writer.close();

        ActiveMQSession session = (ActiveMQSession) connection.createSession(
                false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        MessageConsumer consumer = session.createConsumer(destination);
        BlobMessage message = session.createBlobMessage(file);

        producer.send(message);
        Thread.sleep(1000);

        // check message send
        Message msg = consumer.receive(1000);
        Assert.assertTrue(msg instanceof ActiveMQBlobMessage);

        InputStream input = ((ActiveMQBlobMessage) msg).getInputStream();
        StringBuilder b = new StringBuilder();
        int i = input.read();
        while (i != -1) {
            b.append((char) i);
            i = input.read();
        }
        input.close();
        File uploaded = new File(ftpHomeDirFile, msg.getJMSMessageID().toString().replace(":", "_")); 
        Assert.assertEquals(content, b.toString());
        assertTrue(uploaded.exists());
        ((ActiveMQBlobMessage)msg).deleteFile();
        assertFalse(uploaded.exists());
    }

}
