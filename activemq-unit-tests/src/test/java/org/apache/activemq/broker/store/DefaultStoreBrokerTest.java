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
package org.apache.activemq.broker.store;

import java.net.URI;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;

/**
 * Once the wire format is completed we can test against real persistence storage.
 * 
 * 
 */
public class DefaultStoreBrokerTest extends BrokerTest {

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://()/localhost?deleteAllMessagesOnStartup=true"));
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("broker://()/localhost"));
    }
    
    public static Test suite() {
        return suite(DefaultStoreBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
