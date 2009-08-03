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

import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.commons.net.ftp.FTPClient;

/**
 * A FTP implementation for {@link BlobDownloadStrategy}.
 */
public class FTPBlobDownloadStrategy implements BlobDownloadStrategy {
    private String ftpUser;
    private String ftpPass;

    public InputStream getInputStream(ActiveMQBlobMessage message) throws IOException, JMSException {
        URL url = message.getURL();
        
        setUserInformation(url.getUserInfo());
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
        String file = path.substring(path.lastIndexOf("/")+1);
        
        ftp.changeWorkingDirectory(workingDir);
        ftp.setFileType(FTPClient.BINARY_FILE_TYPE);
        InputStream input = ftp.retrieveFileStream(file);
        ftp.quit();
        ftp.disconnect();
        
        return input;
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
