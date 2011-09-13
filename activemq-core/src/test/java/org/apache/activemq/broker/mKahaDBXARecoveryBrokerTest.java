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
package org.apache.activemq.broker;

import java.util.LinkedList;
import java.util.List;
import javax.jms.JMSException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import junit.framework.Test;
import org.apache.activemq.broker.jmx.RecoveredXATransactionViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DataArrayResponse;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.kahadb.FilteredKahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.store.kahadb.MultiKahaDBPersistenceAdapter;
import org.apache.activemq.util.JMXSupport;

public class mKahaDBXARecoveryBrokerTest extends XARecoveryBrokerTest {

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);

        MultiKahaDBPersistenceAdapter mKahaDB = new MultiKahaDBPersistenceAdapter();
        List adapters = new LinkedList<FilteredKahaDBPersistenceAdapter>();
        FilteredKahaDBPersistenceAdapter defaultEntry = new FilteredKahaDBPersistenceAdapter();
        defaultEntry.setPersistenceAdapter(new KahaDBPersistenceAdapter());
        adapters.add(defaultEntry);

        FilteredKahaDBPersistenceAdapter special = new FilteredKahaDBPersistenceAdapter();
        special.setDestination(new ActiveMQQueue("special"));
        special.setPersistenceAdapter(new KahaDBPersistenceAdapter());
        adapters.add(special);

        mKahaDB.setFilteredPersistenceAdapters(adapters);
        broker.setPersistenceAdapter(mKahaDB);
    }

    public static Test suite() {
        return suite(mKahaDBXARecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue("test,special");
    }

}
