package org.apache.activemq.blob;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.jmock.Mockery;

public class FTPTestSupport extends EmbeddedBrokerTestSupport {
    
    protected static final String ftpServerListenerName = "default";
    protected Connection connection;
    protected FtpServer server;
    String userNamePass = "activemq";

    Mockery context = null;
    String ftpUrl;
    int ftpPort;
    
    final File ftpHomeDirFile = new File("target/FTPBlobTest/ftptest");
    
    protected void setUp() throws Exception {
        
        if (ftpHomeDirFile.getParentFile().exists()) {
            ftpHomeDirFile.getParentFile().delete();
        }
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
        
        // authorize user
        List<Authority> auths = new ArrayList<Authority>();
        Authority auth = new WritePermission();
        auths.add(auth);
        user.setAuthorities(auths);
        
        userManager.save(user);

        BaseUser guest = new BaseUser();
        guest.setName("guest");
        guest.setPassword("guest");
        guest.setHomeDirectory(ftpHomeDirFile.getParent());
        
        userManager.save(guest);
        
        serverFactory.setUserManager(userManager);
        factory.setPort(0);
        serverFactory.addListener(ftpServerListenerName, factory
                .createListener());
        server = serverFactory.createServer();
        server.start();
        ftpPort = serverFactory.getListener(ftpServerListenerName)
                .getPort();
        super.setUp();
    }
    
    public void setConnection() throws Exception {
        ftpUrl = "ftp://"
            + userNamePass
            + ":"
            + userNamePass
            + "@localhost:"
            + ftpPort
            + "/ftptest/";
        bindAddress = "vm://localhost?jms.blobTransferPolicy.defaultUploadUrl=" + ftpUrl;
        
        connectionFactory = createConnectionFactory();
        
        connection = createConnection();
        connection.start();        
    }
    
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.stop();
        }
        super.tearDown();
        if (server != null) {
            server.stop();
        }
    }

    
    
}
