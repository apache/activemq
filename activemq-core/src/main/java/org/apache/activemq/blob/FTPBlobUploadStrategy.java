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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.commons.net.ftp.FTPClient;

/**
 * A FTP implementation of {@link BlobUploadStrategy}.
 */
public class FTPBlobUploadStrategy implements BlobUploadStrategy {
	
	private URL url;
	private String ftpUser = "";
	private String ftpPass = "";
	private BlobTransferPolicy transferPolicy;
	
	public FTPBlobUploadStrategy(BlobTransferPolicy transferPolicy) throws MalformedURLException {
		this.transferPolicy = transferPolicy;
		this.url = new URL(this.transferPolicy.getUploadUrl());
		
		setUserInformation(url.getUserInfo());
	}

	public URL uploadFile(ActiveMQBlobMessage message, File file)
			throws JMSException, IOException {
		return uploadStream(message, new FileInputStream(file));
	}

	public URL uploadStream(ActiveMQBlobMessage message, InputStream in)
			throws JMSException, IOException {
		String connectUrl = url.getHost();
		int port = url.getPort() < 1 ? 21 : url.getPort();
		
		FTPClient ftp = new FTPClient();
		try {
        	ftp.connect(connectUrl, port);
        } catch(ConnectException e) {
        	throw new JMSException("Problem connecting the FTP-server");
        }
		if(!ftp.login(ftpUser, ftpPass)) {
			ftp.quit();
			ftp.disconnect();
			throw new JMSException("Cant Authentificate to FTP-Server");
		}
		String path = url.getPath();
        String workingDir = path.substring(0, path.lastIndexOf("/"));
		String filename = message.getMessageId().toString().replaceAll(":", "_");
        ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
        
        String url;
        if(!ftp.changeWorkingDirectory(workingDir)) {
        	url = this.url.toString().replaceFirst(this.url.getPath(), "")+"/";
        } else {
        	url = this.url.toString();
        }
        
		ftp.storeFile(filename, in);
		ftp.quit();
		ftp.disconnect();
		
		return new URL(url + filename);
	}
	
	private void setUserInformation(String userInfo) {
		if(userInfo != null) {
			String[] userPass = userInfo.split(":");
			if(userPass.length > 0) this.ftpUser = userPass[0];
			if(userPass.length > 1) this.ftpPass = userPass[1];
		} else {
			this.ftpUser = "anonymous";
			this.ftpPass = "anonymous";
		}
	}

}
