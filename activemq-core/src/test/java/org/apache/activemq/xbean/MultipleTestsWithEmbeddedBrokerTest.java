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
package org.apache.activemq.xbean;

import javax.jms.Connection;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.test.retroactive.RetroactiveConsumerWithMessageQueryTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author Neil Clayton
 * @version $Revision$
 */
public class MultipleTestsWithEmbeddedBrokerTest extends EmbeddedBrokerTestSupport {
    private static final Log LOG = LogFactory.getLog(MultipleTestsWithEmbeddedBrokerTest.class);

    protected Connection connection;

    public void test1() throws Exception {
    }

    public void test2() throws Exception {
    }

    protected void setUp() throws Exception {
        LOG.info("### starting up the test case: " + getName());

        super.setUp();
        connection = connectionFactory.createConnection();
        connection.start();
        LOG.info("### started up the test case: " + getName());
    }

    protected void tearDown() throws Exception {
        connection.close();

        super.tearDown();

        LOG.info("### closed down the test case: " + getName());
    }
}
