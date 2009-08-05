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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.BlobMessage;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.AuthorizationRequest;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;

/**
 * To start this test make sure an ftp server is running with user: activemq and
 * password: activemq
 */
public class FTPBlobTest extends EmbeddedBrokerTestSupport {

    private static final String ftpServerListenerName = "default";
    private ActiveMQConnection connection;
    private FtpServer server;
    final static String userNamePass = "activemq";

    Mockery context = null;

    protected void setUp() throws Exception {

        final File ftpHomeDirFile = new File("target/FTPBlobTest/ftptest");
        ftpHomeDirFile.mkdirs();
        ftpHomeDirFile.getParentFile().deleteOnExit();

        FtpServerFactory serverFactory = new FtpServerFactory();
        ListenerFactory factory = new ListenerFactory();

        // mock up a user manager to validate user activemq:activemq and provide
        // home dir options
        context = new Mockery();
        final UserManager userManager = context.mock(UserManager.class);
        final User user = context.mock(User.class);
        context.checking(new Expectations() {{
                atLeast(1).of(userManager).authenticate(
                        with(any(UsernamePasswordAuthentication.class))); will(returnValue(user));
                atLeast(1).of(userManager).getUserByName(userNamePass); will(returnValue(user));
                atLeast(1).of(user).getHomeDirectory(); will(returnValue(ftpHomeDirFile.getParent()));
                atLeast(1).of(user).getMaxIdleTime(); will(returnValue(20000));
                atLeast(1).of(user).getName(); will(returnValue(userNamePass));
                atLeast(1).of(user).authorize( with(any(AuthorizationRequest.class))); will(new CustomAction("return first passed in param") {
                    public Object invoke(Invocation invocation)
                            throws Throwable {
                        return invocation.getParameter(0);
                    }
                });
            }
        });

        serverFactory.setUserManager(userManager);
        factory.setPort(0);
        serverFactory.addListener(ftpServerListenerName, factory
                .createListener());
        server = serverFactory.createServer();
        server.start();
        int ftpPort = serverFactory.getListener(ftpServerListenerName)
                .getPort();

        bindAddress = "vm://localhost?jms.blobTransferPolicy.defaultUploadUrl=ftp://"
                + userNamePass
                + ":"
                + userNamePass
                + "@localhost:"
                + ftpPort
                + "/ftptest/";
        super.setUp();

        connection = (ActiveMQConnection) createConnection();
        connection.start();
    }

    protected void tearDown() throws Exception {
        connection.close();
        super.tearDown();
        server.stop();
    }

    public void testBlobFile() throws Exception {
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
        Assert.assertEquals(content, b.toString());
    }

}
