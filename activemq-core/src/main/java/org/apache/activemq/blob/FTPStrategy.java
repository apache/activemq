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
