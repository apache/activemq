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
package org.apache.activemq.broker.store;

import java.io.File;
import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.XARecoveryBrokerTest;
import org.apache.activemq.store.kahadaptor.KahaPersistentAdaptor;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * Used to verify that recovery works correctly against 
 * 
 * @version $Revision$
 */
public class KahaXARecoveryBrokerTest extends XARecoveryBrokerTest {

    public static Test suite() {
        return suite(KahaXARecoveryBrokerTest.class);
    }
    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = createRestartedBroker();
        broker.setDeleteAllMessagesOnStartup(true);
        return broker;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
       
        KahaPersistentAdaptor adaptor = new KahaPersistentAdaptor(new File("activemq-data/storetest"));
        broker.setPersistenceAdapter(adaptor);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }
    
}
