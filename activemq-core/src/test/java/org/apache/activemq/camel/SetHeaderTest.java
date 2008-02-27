/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.camel;

import java.util.List;

import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.EndpointInject;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.util.ObjectHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit38.AbstractJUnit38SpringContextTests;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @version $Revision: 1.1 $
 */
@ContextConfiguration
public class SetHeaderTest extends AbstractJUnit38SpringContextTests {
    private static final transient Log LOG = LogFactory.getLog(SetHeaderTest.class);

    @Autowired
    protected CamelContext camelContext;

    @EndpointInject(uri = "mock:results")
    protected MockEndpoint expectedEndpoint;

    public void testMocksAreValid() throws Exception {
        // lets add more expectations
        expectedEndpoint.expectedMessageCount(1);
        expectedEndpoint.message(0).header("JMSXGroupID").isEqualTo("ABC");

        MockEndpoint.assertIsSatisfied(camelContext);

        // lets dump the received messages
        List<Exchange> list = expectedEndpoint.getReceivedExchanges();
        for (Exchange exchange : list) {
            Object body = exchange.getIn().getBody();
            LOG.debug("Received: body: " + body + " of type: " + ObjectHelper.className(body) + " on: " + exchange);
        }
    }
}