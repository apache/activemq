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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class TransportUriTest extends EmbeddedBrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(TransportUriTest.class);
    private static final String DIFF_SERV = "&diffServ=";
    private static final String TOS = "&typeOfService=";

    protected Connection connection;
    
    public String prefix;
    public String postfix;


    public void initCombosForTestUriOptionsWork() {
        initSharedCombos();
    }

    public void testUriOptionsWork() throws Exception {
        String uri = prefix + bindAddress + postfix;
        LOG.info("Connecting via: " + uri);

        connection = new ActiveMQConnectionFactory(uri).createConnection();
        connection.start();
    }

    public void initCombosForTestValidDiffServOptionsWork() {
        initSharedCombos();
    }

    public void testValidDiffServOptionsWork() throws Exception {
        String[] validIntegerOptions = {"0", "1", "32", "62", "63"};
        for (String opt : validIntegerOptions) {
            testValidOptionsWork(DIFF_SERV + opt, "");
        }
        String[] validNameOptions = { "CS0", "CS1", "CS2", "CS3", "CS4", "CS5", "CS6",
                "CS7", "EF", "AF11", "AF12","AF13", "AF21", "AF22", "AF23", "AF31", 
                "AF32", "AF33", "AF41", "AF42", "AF43" };
        for (String opt : validNameOptions) {
            testValidOptionsWork(DIFF_SERV + opt, "");
        }
    }

    public void initCombosForTestInvalidDiffServOptionDoesNotWork() {
        initSharedCombos();
    }

    public void testInvalidDiffServOptionsDoesNotWork() throws Exception {
        String[] invalidIntegerOptions = {"-2", "-1", "64", "65", "100", "255"};
        for (String opt : invalidIntegerOptions) {
            testInvalidOptionsDoNotWork(DIFF_SERV + opt, "");
        }
        String[] invalidNameOptions = {"hi", "", "A", "AF", "-AF21"};
        for (String opt : invalidNameOptions) {
            testInvalidOptionsDoNotWork(DIFF_SERV + opt, "");
        }
    }

    public void initCombosForTestValidTypeOfServiceOptionsWork() {
        initSharedCombos();
    }

    public void testValidTypeOfServiceOptionsWork() throws Exception {
        int[] validOptions = {0, 1, 32, 100, 254, 255};
        for (int opt : validOptions) {
            testValidOptionsWork(TOS + opt, "");
        }
    }

    public void initCombosForTestInvalidTypeOfServiceOptionDoesNotWork() {
        initSharedCombos();
    }

    public void testInvalidTypeOfServiceOptionDoesNotWork() throws Exception {
        int[] invalidOptions = {-2, -1, 256, 257};
        for (int opt : invalidOptions) {
            testInvalidOptionsDoNotWork(TOS + opt, "");
        }
    }

    public void initCombosForTestDiffServAndTypeOfServiceMutuallyExclusive() {
        initSharedCombos();
    }

    public void testDiffServAndTypeServiceMutuallyExclusive() {
        String msg = "It should not be possible to set both Differentiated "
            + "Services and Type of Service options on the same connection "
            + "URI.";
        testInvalidOptionsDoNotWork(TOS + 32 + DIFF_SERV, msg);
        testInvalidOptionsDoNotWork(DIFF_SERV + 32 + TOS + 32, msg);
    }

    public void initCombosForTestBadVersionNumberDoesNotWork() {
        initSharedCombos();
    }

    public void testBadVersionNumberDoesNotWork() throws Exception {
        testInvalidOptionsDoNotWork("&minmumWireFormatVersion=65535", "");
    }

    public void initCombosForTestBadPropertyNameFails() {
        initSharedCombos();
    }
        
    public void testBadPropertyNameFails() throws Exception {
        testInvalidOptionsDoNotWork("&cheese=abc", "");
    }

    private void initSharedCombos() {
        addCombinationValues("prefix", new Object[] {""});
        // TODO: Add more combinations.
        addCombinationValues("postfix", new Object[]
            {"?tcpNoDelay=true&keepAlive=true&soLinger=0"});
    }

    private void testValidOptionsWork(String options, String msg) {
        String uri = prefix + bindAddress + postfix + options;
        LOG.info("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
        } catch (Exception unexpected) {
            fail("Valid options '" + options + "' on URI '" + uri + "' should "
                 + "not have caused an exception to be thrown. " + msg
                 + " Exception: " + unexpected);
        }
    }

    private void testInvalidOptionsDoNotWork(String options, String msg) {
        String uri = prefix + bindAddress + postfix + options;
        LOG.info("Connecting via: " + uri);

        try {
            connection = new ActiveMQConnectionFactory(uri).createConnection();
            connection.start();
            fail("Invalid options '" + options + "' on URI '" + uri + "' should"
                 + " have caused an exception to be thrown. " + msg);
        } catch (Exception expected) {
        }
    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:61616";
        super.setUp();
    }

    @Override
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

    @Override
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
