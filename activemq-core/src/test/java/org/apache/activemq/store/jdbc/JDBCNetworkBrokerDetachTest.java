package org.apache.activemq.store.jdbc;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.network.NetworkBrokerDetachTest;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCNetworkBrokerDetachTest extends NetworkBrokerDetachTest {

    protected void configureBroker(BrokerService broker) throws Exception {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        EmbeddedDataSource dataSource = new EmbeddedDataSource();
        dataSource.setDatabaseName(broker.getBrokerName());
        dataSource.setCreateDatabase("create");
        jdbc.setDataSource(dataSource);
        jdbc.deleteAllMessages();
        broker.setPersistenceAdapter(jdbc);
    }
	
}
