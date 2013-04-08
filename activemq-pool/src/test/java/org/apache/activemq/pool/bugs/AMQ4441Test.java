package org.apache.activemq.pool.bugs;

import static org.junit.Assert.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.pool.PooledConnection;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4441Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4441Test.class);
    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();
        broker.waitUntilStarted();
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout=120000)
    public void demo() throws JMSException, InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean done = new AtomicBoolean(false);
        final PooledConnectionFactory pooled = new PooledConnectionFactory("vm://localhost?create=false");

        pooled.setMaxConnections(2);
        pooled.setExpiryTimeout(10L);
        pooled.start();
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    while (!done.get() && latch.getCount() > 0) {
                        try {
                            final PooledConnection pooledConnection = (PooledConnection) pooled.createConnection();
                            if (pooledConnection.getConnection() == null) {
                                LOG.info("Found broken connection.");
                                latch.countDown();
                            }
                            pooledConnection.close();
                        } catch (JMSException e) {
                            LOG.warn("Caught Exception", e);
                        }
                    }
                }
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }

        if (latch.await(1, TimeUnit.MINUTES)) {
            fail("A thread obtained broken connection");
        }

        done.set(true);
        for (Thread thread : threads) {
            thread.join();
        }
    }

}
