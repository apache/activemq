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
import java.io.InputStream;
import java.net.URL;

import javax.jms.JMSException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.jmock.Mockery;

public class FTPBlobDownloadStrategyTest extends TestCase {

    private static final String ftpServerListenerName = "default";
    private FtpServer server;
    final static String userNamePass = "activemq";

    Mockery context = null;
    int ftpPort;
    String ftpUrl;

    final int FILE_SIZE = Short.MAX_VALUE * 10;

    protected void setUp() throws Exception {
        final File ftpHomeDirFile = new File("target/FTPBlobTest/ftptest");
        ftpHomeDirFile.mkdirs();
        ftpHomeDirFile.getParentFile().deleteOnExit();

        FtpServerFactory serverFactory = new FtpServerFactory();
        ListenerFactory factory = new ListenerFactory();

        PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        UserManager userManager = userManagerFactory.createUserManager();

        BaseUser user = new BaseUser();
        user.setName("activemq");
        user.setPassword("activemq");
        user.setHomeDirectory(ftpHomeDirFile.getParent());

        userManager.save(user);

        serverFactory.setUserManager(userManager);
        factory.setPort(0);
        serverFactory.addListener(ftpServerListenerName, factory
                .createListener());
        server = serverFactory.createServer();
        server.start();
        ftpPort = serverFactory.getListener(ftpServerListenerName)
                .getPort();

        ftpUrl = "ftp://" + userNamePass + ":" + userNamePass + "@localhost:"
                + ftpPort + "/ftptest/";

        File uploadFile = new File(ftpHomeDirFile, "test.txt");
        FileWriter wrt = new FileWriter(uploadFile);

        wrt.write("hello world");

        for(int ix = 0; ix < FILE_SIZE; ++ix ) {
            wrt.write("a");
        }

        wrt.close();

    }

    public void testDownload() {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy();
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
            Assert.assertEquals("hello world", sb.toString().substring(0, "hello world".length()));
            Assert.assertEquals(FILE_SIZE, sb.toString().substring("hello world".length()).length());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }

    public void testWrongAuthentification() {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy();
        try {
            message.setURL(new URL("ftp://" + userNamePass + "_wrong:" + userNamePass + "@localhost:"	+ ftpPort + "/ftptest/"));
            strategy.getInputStream(message);
        } catch(JMSException e) {
            Assert.assertEquals("Wrong Exception", "Cant Authentificate to FTP-Server", e.getMessage());
            return;
        } catch(Exception e) {
            System.out.println(e);
            Assert.assertTrue("Wrong Exception "+ e, false);
            return;
        }

        Assert.assertTrue("Expect Exception", false);
    }

    public void testWrongFTPPort() {
        ActiveMQBlobMessage message = new ActiveMQBlobMessage();
        BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy();
        try {
            message.setURL(new URL("ftp://" + userNamePass + ":" + userNamePass + "@localhost:"	+ 422 + "/ftptest/"));
            strategy.getInputStream(message);
        } catch(JMSException e) {
            Assert.assertEquals("Wrong Exception", "Problem connecting the FTP-server", e.getMessage());
            return;
        } catch(Exception e) {
            e.printStackTrace();
            Assert.assertTrue("Wrong Exception "+ e, false);
            return;
        }

        Assert.assertTrue("Expect Exception", false);
    }

}
