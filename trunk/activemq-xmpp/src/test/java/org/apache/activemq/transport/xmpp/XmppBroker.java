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
package org.apache.activemq.transport.xmpp;

import org.apache.activemq.Service;
import org.apache.activemq.broker.BrokerService;

/**
 * 
 */
public class XmppBroker implements Service {
    private BrokerService broker = new BrokerService();

    public static void main(String[] args) {
        try {
            XmppBroker broker = new XmppBroker();
            broker.start();

            System.out.println("Press any key to terminate");
            System.in.read();
        } catch (Exception e) {
            System.out.println("Caught: " + e);
            e.printStackTrace();
        }
    }

    public void start() throws Exception {
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("xmpp://localhost:61222");
        broker.start();
    }

    public void stop() throws Exception {
        broker.stop();
    }
}
