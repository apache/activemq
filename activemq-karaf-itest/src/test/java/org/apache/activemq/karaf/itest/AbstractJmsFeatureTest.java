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
package org.apache.activemq.karaf.itest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class AbstractJmsFeatureTest extends AbstractFeatureTest {

    public static void copyFile(File from, File to) throws IOException {
        if (!to.exists()) {
            System.err.println("Creating new file for: "+ to);
            to.createNewFile();
        }
        FileChannel in = new FileInputStream(from).getChannel();
        FileChannel out = new FileOutputStream(to).getChannel();
        try {
            long size = in.size();
            long position = 0;
            while (position < size) {
                position += in.transferTo(position, 8192, out);
            }
        } finally {
            try {
                in.close();
                out.force(true);
                out.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    protected String consumeMessage(String nameAndPayload) throws Throwable {
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(session.createQueue(nameAndPayload));
        TextMessage message = (TextMessage) consumer.receive(10000);
        System.err.println("message: " + message);
        connection.close();
        return message.getText();
    }

    protected void produceMessage(String nameAndPayload) throws Throwable{
        Connection connection = getConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(session.createQueue(nameAndPayload)).send(session.createTextMessage(nameAndPayload));
        connection.close();
    }

    protected Connection getConnection() throws Throwable {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory();
        Connection connection = factory.createConnection(AbstractFeatureTest.USER, AbstractFeatureTest.PASSWORD);
        connection.start();

        return connection;
    }
}