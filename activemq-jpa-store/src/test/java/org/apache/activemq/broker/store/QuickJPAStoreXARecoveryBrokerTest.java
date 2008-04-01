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

import java.util.Properties;
import junit.framework.Test;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.XARecoveryBrokerTest;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.store.jpa.JPAReferenceStoreAdapter;

/**
 * Used to verify that recovery works correctly against 
 * 
 * @version $Revision$
 */
public class QuickJPAStoreXARecoveryBrokerTest extends XARecoveryBrokerTest {

    public static Test suite() {
        return suite(QuickJPAStoreXARecoveryBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setDeleteAllMessagesOnStartup(true);
        AMQPersistenceAdapter pa = new AMQPersistenceAdapter();
        
        JPAReferenceStoreAdapter rfa = new JPAReferenceStoreAdapter();
        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        props.setProperty("openjpa.ConnectionURL", "jdbc:derby:activemq-data/derby;create=true");
        props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema");
//        props.setProperty("openjpa.Log", "DefaultLevel=WARN,SQL=TRACE");
        rfa.setEntityManagerProperties(props);
        pa.setReferenceStoreAdapter(rfa);        
        
        service.setPersistenceAdapter(pa);
        return service;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService service = new BrokerService();
        AMQPersistenceAdapter pa = new AMQPersistenceAdapter();
        
        JPAReferenceStoreAdapter rfa = new JPAReferenceStoreAdapter();
        Properties props = new Properties();
        props.setProperty("openjpa.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver");
        props.setProperty("openjpa.ConnectionURL", "jdbc:derby:activemq-data/derby;create=true");
        props.setProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema");
//        props.setProperty("openjpa.Log", "DefaultLevel=WARN,SQL=TRACE");
        rfa.setEntityManagerProperties(props);
        pa.setReferenceStoreAdapter(rfa);        
        
        service.setPersistenceAdapter(pa);
        return service;
    }
    
}
