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

import java.io.InputStream;
import java.net.URL;

import javax.jms.JMSException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQBlobMessage;

/**
 * To start this test make sure an ftp server is running with
 * user: activemq and password: activemq. 
 * Also a file called test.txt with the content <b>hello world</b> must be in the ftptest directory.
 */
public class FTPBlobDownloadStrategyTest extends TestCase {
	
	public void xtestDownload() {
		ActiveMQBlobMessage message = new ActiveMQBlobMessage();
		BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy();
		InputStream stream;
		try {
			message.setURL(new URL("ftp://activemq:activemq@localhost/ftptest/test.txt"));
			stream = strategy.getInputStream(message);
			int i = stream.read();
			StringBuilder sb = new StringBuilder(10);
			while(i != -1) {
				sb.append((char)i);
				i = stream.read();
			}
			Assert.assertEquals("hello world", sb.toString());
		} catch (Exception e) {
			e.printStackTrace();
			Assert.assertTrue(false);
		}
	}
	
	public void xtestWrongAuthentification() {
		ActiveMQBlobMessage message = new ActiveMQBlobMessage();
		BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy();
		try {
			message.setURL(new URL("ftp://activemq:activemq_wrong@localhost/ftptest/test.txt"));
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
	
	public void xtestWrongFTPPort() {
		ActiveMQBlobMessage message = new ActiveMQBlobMessage();
		BlobDownloadStrategy strategy = new FTPBlobDownloadStrategy();
		try {
			message.setURL(new URL("ftp://activemq:activemq@localhost:442/ftptest/test.txt"));
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
