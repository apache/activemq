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
package org.apache.activemq.broker.store;

import java.io.File;
import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.RecoveryBrokerTest;
import org.apache.activemq.store.kahadaptor.KahaPersistenceAdapter;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;

/**
 * Used to verify that recovery works correctly against 
 * 
 * @version $Revision$
 */
public class KahaRecoveryBrokerTest extends RecoveryBrokerTest {
   
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = createRestartedBroker();
        broker.setDeleteAllMessagesOnStartup(true);
        return broker;
    }
    
    protected BrokerService createRestartedBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaPersistenceAdapter adaptor = new KahaPersistenceAdapter(new File( System.getProperty("basedir", ".")+"/target/activemq-data/kaha-store.db"));
        broker.setPersistenceAdapter(adaptor);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    public static Test suite() {
        return suite(KahaRecoveryBrokerTest.class);
    }

    
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
