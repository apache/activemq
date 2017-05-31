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
package org.apache.activemq.transport.amqp;

import java.io.File;

import javax.jms.Connection;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.kahadb.KahaDBStore;

public class IDERunner {

    private static final String AMQP_TRANSFORMER = "jms";
    private static final boolean TRANSPORT_TRACE = false;
    private static final boolean PERSISTENT = true;
    private static final boolean CLIENT_CONNECT = false;

    public static void main(String[]args) throws Exception {
        BrokerService brokerService = new BrokerService();

        TransportConnector connector = brokerService.addConnector(
            "amqp://0.0.0.0:5672?trace=" + TRANSPORT_TRACE +
                "&transport.transformer=" + AMQP_TRANSFORMER +
                "&transport.wireFormat.maxFrameSize=104857600");

        KahaDBStore store = new KahaDBStore();
        store.setDirectory(new File("target/activemq-data/kahadb"));

        if (PERSISTENT) {
            brokerService.setStoreOpenWireVersion(10);
            brokerService.setPersistenceAdapter(store);
            brokerService.deleteAllMessages();
        } else {
            brokerService.setPersistent(false);
        }

        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);

        brokerService.start();

        if (CLIENT_CONNECT) {
            Connection connection = JMSClientContext.INSTANCE.createConnection(connector.getPublishableConnectURI());
            connection.start();
        }

        brokerService.waitUntilStopped();
    }
}
