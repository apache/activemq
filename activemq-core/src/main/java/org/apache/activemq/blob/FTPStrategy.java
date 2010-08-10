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
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;

import javax.jms.JMSException;

import org.apache.activemq.command.ActiveMQBlobMessage;
import org.apache.commons.net.ftp.FTPClient;

public class FTPStrategy {

    protected BlobTransferPolicy transferPolicy;
    protected URL url;
    protected String ftpUser = "";
    protected String ftpPass = "";

    public FTPStrategy(BlobTransferPolicy transferPolicy) throws MalformedURLException {
        this.transferPolicy = transferPolicy;
        this.url = new URL(this.transferPolicy.getUploadUrl());
    }
    
    protected void setUserInformation(String userInfo) {
        if(userInfo != null) {
            String[] userPass = userInfo.split(":");
            if(userPass.length > 0) this.ftpUser = userPass[0];
            if(userPass.length > 1) this.ftpPass = userPass[1];
        } else {
            this.ftpUser = "anonymous";
            this.ftpPass = "anonymous";
        }
    }
    
    protected FTPClient createFTP() throws IOException, JMSException {
        String connectUrl = url.getHost();
        setUserInformation(url.getUserInfo());
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
        return ftp;
    }
    
}
