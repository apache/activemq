/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.xbean;

import org.apache.activemq.EmbeddedBrokerTestSupport;

import javax.jms.Connection;

/**
 * 
 * @author Neil Clayton
 * @version $Revision$
 */
public class MultipleTestsWithEmbeddedBrokerTest extends EmbeddedBrokerTestSupport {
    protected Connection connection;

    public void test1() throws Exception {
    }

    public void test2() throws Exception {
    }

    protected void setUp() throws Exception {
        log.info("### starting up the test case: " + getName());

        super.setUp();
        connection = connectionFactory.createConnection();
        connection.start();
        log.info("### started up the test case: " + getName());
    }

    protected void tearDown() throws Exception {
        connection.close();

        super.tearDown();

        log.info("### closed down the test case: " + getName());
    }
}
