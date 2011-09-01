package org.apache.activemq.pool;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;

import org.apache.log4j.Logger;


/**
 * Checks the behavior of the PooledConnectionFactory when the maximum amount
 * of sessions is being reached.
 * Older versions simply block in the call to Connection.getSession(), which isn't good.
 * An exception being returned is the better option, so JMS clients don't block.
 * This test succeeds if an exception is returned and fails if the call to getSession()
 * blocks.
 *
 */
public class PooledConnectionFactoryTest extends TestCase
{
    public final static Logger LOG = Logger.getLogger(PooledConnectionFactoryTest.class);


    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public PooledConnectionFactoryTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( PooledConnectionFactoryTest.class );
    }

    /**
     * Tests the behavior of the sessionPool of the PooledConnectionFactory
     * when maximum number of sessions are reached.
     */
    public void testApp() throws Exception
    {
        // using separate thread for testing so that we can interrupt the test
        // if the call to get a new session blocks.

        // start test runner thread
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> result = (Future<Boolean>) executor.submit(new TestRunner());

        // test should not take > 5secs, so test fails i
        Thread.sleep(5*1000);

        if (!result.isDone() || !result.get().booleanValue()) {
            PooledConnectionFactoryTest.LOG.error("2nd call to createSession()" +
            " is blocking but should have returned an error instead.");

            executor.shutdownNow();

            Assert.fail("SessionPool inside PooledConnectionFactory is blocking if " +
            "limit is exceeded but should return an exception instead.");
        }
    }
}

class TestRunner implements Callable<Boolean> {

    public final static Logger LOG = Logger.getLogger(TestRunner.class);

    /**
     * @return true if test succeeded, false otherwise
     */
    public Boolean call() {

        Connection conn = null;
        Session one = null;

        // wait at most 5 seconds for the call to createSession
        try {
            ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory("vm://broker1?marshal=false&broker.persistent=false");
            PooledConnectionFactory cf = new PooledConnectionFactory(amq);
            cf.setMaxConnections(3);
            cf.setMaximumActive(1);
            cf.setBlockIfSessionPoolIsFull(false);

            conn = cf.createConnection();
            one = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            Session two = null;
            try {
                // this should raise an exception as we called setMaximumActive(1)
                two = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
                two.close();

                LOG.error("Expected JMSException wasn't thrown.");
                Assert.fail("seconds call to Connection.createSession() was supposed" +
                        "to raise an JMSException as internal session pool" +
                        "is exhausted. This did not happen and indiates a problem");
                return new Boolean(false);
            } catch (JMSException ex) {
                if (ex.getCause().getClass() == java.util.NoSuchElementException.class) {
                    //expected, ignore but log
                    LOG.info("Caught expected " + ex);
                } else {
                    LOG.error(ex);
                    return new Boolean(false);
                }
            } finally {
                if (one != null)
                    one.close();
                if (conn != null)
                    conn.close();
            }
        } catch (Exception ex) {
            LOG.error(ex.getMessage());
            return new Boolean(false);
        }

        // all good, test succeeded
        return new Boolean(true);
    }
}
