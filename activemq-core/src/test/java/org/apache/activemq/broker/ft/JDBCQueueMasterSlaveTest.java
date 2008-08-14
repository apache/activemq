package org.apache.activemq.broker.ft;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.jdbc.DataSourceSupport;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.derby.jdbc.EmbeddedDataSource;

public class JDBCQueueMasterSlaveTest extends QueueMasterSlaveTest {
    protected EmbeddedDataSource sharedDs;
    protected String MASTER_URL = "tcp://localhost:62001";
    protected String SLAVE_URL  = "tcp://localhost:62002";

    protected void setUp() throws Exception {
        // startup db
        sharedDs = (EmbeddedDataSource) new DataSourceSupport().getDataSource();
        super.setUp();
    }
    
    protected void createMaster() throws Exception {
        master = new BrokerService();
        master.addConnector(MASTER_URL);
        master.setUseJmx(false);
        master.setPersistent(true);
        master.setDeleteAllMessagesOnStartup(true);
        JDBCPersistenceAdapter persistenceAdapter = new JDBCPersistenceAdapter();
        persistenceAdapter.setDataSource(getExistingDataSource());
        persistenceAdapter.setLockKeepAlivePeriod(500);
        master.setPersistenceAdapter(persistenceAdapter);
        master.start();
    }

    protected void createSlave() throws Exception {
        // use a separate thread as the slave will block waiting for
        // the exclusive db lock
        Thread t = new Thread() {
            public void run() {
                try {
                    BrokerService broker = new BrokerService();
                    broker.addConnector(SLAVE_URL);
                    // no need for broker.setMasterConnectorURI(masterConnectorURI)
                    // as the db lock provides the slave/master initialisation
                    broker.setUseJmx(false);
                    broker.setPersistent(true);
                    JDBCPersistenceAdapter persistenceAdapter = new JDBCPersistenceAdapter();
                    persistenceAdapter.setDataSource(getExistingDataSource());
                    persistenceAdapter.setCreateTablesOnStartup(false);
                    broker.setPersistenceAdapter(persistenceAdapter);
                    broker.start();
                    slave.set(broker);
                    slaveStarted.countDown();
                } catch (Exception e) {
                    fail("failed to start slave broker, reason:" + e);
                }
            }
        };
        t.start();
    }

    protected EmbeddedDataSource getExistingDataSource() throws Exception {
        return sharedDs;
    }
}
