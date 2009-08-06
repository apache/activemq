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
import java.net.URL;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import junit.framework.Assert;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.activemq.command.MessageId;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.AuthorizationRequest;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.UsernamePasswordAuthentication;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.api.Invocation;
import org.jmock.lib.action.CustomAction;


public class FTPBlobUploadStrategyTest extends EmbeddedBrokerTestSupport {
	
    private static final String ftpServerListenerName = "default";
    private Connection connection;
    private FtpServer server;
    final static String userNamePass = "activemq";

	Mockery context = null;
	String ftpUrl;
	
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
        int ftpPort = serverFactory.getListener(ftpServerListenerName)
                .getPort();
		
        ftpUrl = "ftp://"
            + userNamePass
            + ":"
            + userNamePass
            + "@localhost:"
            + ftpPort
            + "/ftptest/";
        bindAddress = "vm://localhost?jms.blobTransferPolicy.defaultUploadUrl=" + ftpUrl;
        super.setUp();

        connection = createConnection();
        connection.start();
        
        // check if file exist and delete it
        URL url = new URL(ftpUrl);
        String connectUrl = url.getHost();
		int port = url.getPort() < 1 ? 21 : url.getPort();
		
		FTPClient ftp = new FTPClient();
		ftp.connect(connectUrl, port);
		if(!ftp.login("activemq", "activemq")) {
			ftp.quit();
			ftp.disconnect();
			throw new JMSException("Cant Authentificate to FTP-Server");
		}
		ftp.changeWorkingDirectory("ftptest");
		ftp.deleteFile("testmessage");
		ftp.quit();
		ftp.disconnect();
    }
	
	public void testFileUpload() throws Exception {
		File file = File.createTempFile("amq-data-file-", ".dat");
        // lets write some data
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.append("hello world");
        writer.close();
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ((ActiveMQConnection)connection).setCopyMessageOnSend(false);
        
        ActiveMQBlobMessage message = (ActiveMQBlobMessage) ((ActiveMQSession)session).createBlobMessage(file);
        message.setMessageId(new MessageId("testmessage"));
        message.onSend();
        Assert.assertEquals(ftpUrl + "testmessage", message.getURL().toString()); 
	}

}
