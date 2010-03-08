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
package org.apache.activemq.transport.tcp;

import javax.jms.Connection;
import javax.jms.JMSException;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public class TransportUriTest extends EmbeddedBrokerTestSupport {

	private static final Log LOG = LogFactory.getLog(TransportUriTest.class);
	
    protected Connection connection;
    
    public String prefix;
    public String postfix;
    
    public void initCombosForTestUriOptionsWork() {
		addCombinationValues("prefix", new Object[] {""});
		addCombinationValues("postfix", new Object[] {"?tcpNoDelay=true&keepAlive=true"});
	}

    public void testUriOptionsWork() throws Exception {
        String uri = prefix + bindAddress + postfix;
        LOG.info("Connecting via: " + uri);

        connection = new ActiveMQConnectionFactory(uri).createConnection();
        connection.start();
    }

    public void initCombosForTestValidDiffServOptionsWork() {
	addCombinationValues("prefix", new Object[] {""});
	// TODO: Add more combinations so that we know it plays nice with other
	// transport options.
	addCombinationValues("postfix", new Object[] {"?tcpNoDelay=true&keepAlive=true"});
    }

    public void testValidDiffServOptionsWork() throws Exception {
        String[] validIntegerOptions = {"0", "1", "32", "62", "63"};
        for (String opt : validIntegerOptions) {
            testValidDiffServOption(opt);
        }
        String[] validNameOptions = {"EF", "AF11", "AF12", "AF13", "AF21",
                                     "AF22", "AF23", "AF31", "AF32", "AF33",
                                     "AF41", "AF42", "AF43"};
        for (String opt : validNameOptions) {
            testValidDiffServOption(opt);
        }
    }

    private void testValidDiffServOption(String value) {
        String uri = prefix + bindAddress + postfix + "&diffServ=" + value;
        LOG.info("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
        } catch (Exception e) {
	    fail("Valid Differentiated Services option: diffServ=" + value
		 + ", should not have thrown an exception: " + e);
        }
    }

    public void initCombosForTestInvalidDiffServOptionDoesNotWork() {
	addCombinationValues("prefix", new Object[] {""});
	// TODO: Add more combinations so that we know it plays nice with other
	// transport options.
	addCombinationValues("postfix", new Object[] {"?tcpNoDelay=true&keepAlive=true"});
    }

    public void testInvalidDiffServOptionsDoesNotWork() throws Exception {
        String[] invalidIntegerOptions = {"-2", "-1", "64", "65", "100", "255"};
        for (String opt : invalidIntegerOptions) {
            testInvalidDiffServOption(opt);
        }
        String[] invalidNameOptions = {"hi", "", "A", "AF", "-AF21"};
        for (String opt : invalidNameOptions) {
            testInvalidDiffServOption(opt);
        }
    }

    private void testInvalidDiffServOption(String value) {
        String uri = prefix + bindAddress + postfix + "&diffServ=" + value;
        LOG.info("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
            fail("Invalid Differentiated Services option: diffServ=" + value
                 + " should have thrown an exception!");
        } catch (Exception expected) {
        }
    }

	public void initCombosForTestBadVersionNumberDoesNotWork() {
		addCombinationValues("prefix", new Object[] {""});
		addCombinationValues("postfix", new Object[] {"?tcpNoDelay=true&keepAlive=true"});
	}

    public void testBadVersionNumberDoesNotWork() throws Exception {
        String uri = prefix + bindAddress + postfix + "&minmumWireFormatVersion=65535";
        LOG.info("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
            fail("Should have thrown an exception!");
        } catch (Exception expected) {
        }
    }

	public void initCombosForTestBadPropertyNameFails() {
		addCombinationValues("prefix", new Object[] {""});
		addCombinationValues("postfix", new Object[] {"?tcpNoDelay=true&keepAlive=true"});
	}
	
    public void testBadPropertyNameFails() throws Exception {
        String uri = prefix + bindAddress + postfix + "&cheese=abc";
        LOG.info("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
            fail("Should have thrown an exception!");
        } catch (Exception expected) {
        }
    }

    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                e.printStackTrace();
            }
        }
        super.tearDown();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setPersistent(isPersistent());
        answer.addConnector(bindAddress);
        return answer;
    }
    
    public static Test suite() {
    	return suite(TransportUriTest.class);
    }
}
