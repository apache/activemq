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

package org.apache.activemq.transport.ws;

import java.net.URI;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.Test;


public class WSTransportTest {
    
    protected String getBindLocation() {
        return "ws://localhost:61614";
    }

    @Test
    public void testBrokerStart() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:()/localhost?persistent=false&useJmx=false"));
        broker.addConnector(getBindLocation());
        broker.start();
        broker.waitUntilStarted();
        Thread.sleep(2000);
        //System.in.read();
        broker.stop();
        broker.waitUntilStopped();
    }
    
}
