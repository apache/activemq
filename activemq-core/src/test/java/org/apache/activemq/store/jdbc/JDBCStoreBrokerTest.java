package org.apache.activemq.store.jdbc;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCStoreBrokerTest extends BrokerTest {

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        
        jdbc.deleteAllMessages();
        broker.setPersistenceAdapter(jdbc);
        return broker;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName("derbyDb");
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        broker.setPersistenceAdapter(jdbc);
        return broker;
    }
    
    
    public static Test suite() {
        return suite(JDBCStoreBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
	
}
