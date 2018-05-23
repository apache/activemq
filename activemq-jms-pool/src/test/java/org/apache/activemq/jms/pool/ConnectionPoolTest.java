package org.apache.activemq.jms.pool;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;

import static org.junit.Assert.assertFalse;

public class ConnectionPoolTest extends JmsPoolTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ConnectionPoolTest.class);


    private class PooledConnectionFactoryTest extends PooledConnectionFactory {
        ConnectionPool pool = null;
        @Override
        protected Connection newPooledConnection(ConnectionPool connection) {
            connection.setIdleTimeout(Integer.MAX_VALUE);
            this.pool = connection;
            Connection ret = super.newPooledConnection(connection);
            ConnectionPool cp = ((PooledConnection) ret).pool;
            cp.decrementReferenceCount();
            // will fail if timeout does overflow
            assertFalse(cp.expiredCheck());
            return ret;
        }

        public ConnectionPool getPool() {
            return pool;
        }

    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setSchedulerSupport(false);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @Test(timeout = 120000)
    public void demo() throws JMSException, InterruptedException {
        final PooledConnectionFactoryTest pooled = new PooledConnectionFactoryTest();
        pooled.setConnectionFactory(new ActiveMQConnectionFactory("vm://localhost?create=false"));
        pooled.setMaxConnections(2);
        pooled.setExpiryTimeout(Long.MAX_VALUE);
        pooled.start();
    }
}
