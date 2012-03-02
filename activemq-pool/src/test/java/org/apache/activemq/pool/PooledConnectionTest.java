/**
 * 
 */
package org.apache.activemq.pool;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.IllegalStateException;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A couple of tests against the PooledConnection class.
 *
 */
public class PooledConnectionTest extends TestCase {
	
	private Logger log = LoggerFactory.getLogger(PooledConnectionTest.class);
	
	
	@Override
	public void setUp() throws Exception {
		log.debug("setUp() called.");
	}
	
	
	@Override
	public void tearDown() throws Exception {
		log.debug("tearDown() called.");
	}
		
	
	/**
	 * AMQ-3752:
	 * Tests how the ActiveMQConnection reacts to repeated calls to
	 * setClientID(). 
	 * 
	 * @throws Exception
	 */
	public void testRepeatedSetClientIDCalls() throws Exception {
		log.debug("running testRepeatedSetClientIDCalls()");
		
		// 1st test: call setClientID("newID") twice 
		// this should be tolerated and not result in an exception
		//
		ConnectionFactory cf = createPooledConnectionFactory();
		Connection conn = cf.createConnection();
		conn.setClientID("newID");
		
		try {
			conn.setClientID("newID");
			conn.start();
			conn.close();
			cf = null;
		} catch (IllegalStateException ise) {
			log.error("Repeated calls to ActiveMQConnection.setClientID(\"newID\") caused " + ise.getMessage());
			Assert.fail("Repeated calls to ActiveMQConnection.setClientID(\"newID\") caused " + ise.getMessage());
		}
		
		// 2nd test: call setClientID() twice with different IDs
		// this should result in an IllegalStateException
		//
		cf = createPooledConnectionFactory();
		conn = cf.createConnection();
		conn.setClientID("newID1");
		try {
			conn.setClientID("newID2");
			Assert.fail("calling ActiveMQConnection.setClientID() twice with different clientID must raise an IllegalStateException"); 
		} catch (IllegalStateException ise) {
			log.debug("Correctly received " + ise);
		}
		
		// 3rd test: try to call setClientID() after start()
		// should result in an exception
		cf = createPooledConnectionFactory();
		conn = cf.createConnection();
		try {
		conn.start();
		conn.setClientID("newID3");
		Assert.fail("Calling setClientID() after start() mut raise a JMSException.");
		} catch (IllegalStateException ise) {
			log.debug("Correctly received " + ise);
		}
		
		log.debug("Test finished.");
	}
	
	
	protected ConnectionFactory createPooledConnectionFactory() {
		ConnectionFactory cf = new PooledConnectionFactory("vm://localhost?broker.persistent=false");
		((PooledConnectionFactory)cf).setMaxConnections(1);	
		log.debug("ConnectionFactory initialized.");
		return cf;
	}
}
