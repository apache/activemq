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
package org.apache.activemq.usecases;

import java.util.Set;

import javax.jms.Destination;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerAndConnectionTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @version $Revision: $
 */
public class NewConsumerCreatesDestinationTest extends EmbeddedBrokerAndConnectionTestSupport {
    private static final Log LOG = LogFactory.getLog(NewConsumerCreatesDestinationTest.class);

    private ActiveMQQueue wildcard;
    
    public void testNewConsumerCausesNewDestinationToBeAutoCreated() throws Exception {

        // lets create a wildcard thats kinda like those used by Virtual Topics
        String wildcardText = "org.*" + getDestinationString().substring("org.apache".length());
        wildcard = new ActiveMQQueue(wildcardText);

        LOG.info("Using wildcard: " + wildcard);
        LOG.info("on destination: " + destination);
        
        assertDestinationCreated(destination, false);
        assertDestinationCreated(wildcard, false);
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createConsumer(destination);

        assertDestinationCreated(destination, true);
        assertDestinationCreated(wildcard, true);
    }

    protected void assertDestinationCreated(Destination destination, boolean expected) throws Exception {
        Set answer = broker.getBroker().getDestinations((ActiveMQDestination) destination);
        int size = expected ? 1 : 0;
        assertEquals("Size of found destinations: " + answer, size, answer.size());
    }
}
