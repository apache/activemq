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
package org.apache.activemq.store.jdbc;

import java.io.IOException;

import junit.framework.AssertionFailedError;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterTestSupport;

public class JDBCPersistenceAdapterTest extends PersistenceAdapterTestSupport {
    
    protected PersistenceAdapter createPersistenceAdapter(boolean delete) throws IOException {
        JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        
        // explicitly enable audit as it is now off by default
        // due to org.apache.activemq.broker.ProducerBrokerExchange.canDispatch(Message)
        jdbc.setEnableAudit(true);
        
        brokerService.setSchedulerSupport(false);
        brokerService.setPersistenceAdapter(jdbc);
        if( delete ) {
            jdbc.deleteAllMessages();
        }
        return jdbc;
    }
    
    public void testAuditOff() throws Exception {
        pa.stop();
        pa = createPersistenceAdapter(true);
        ((JDBCPersistenceAdapter)pa).setEnableAudit(false);
        pa.start();
    	boolean failed = true;
    	try {
    		testStoreCanHandleDupMessages();
    		failed = false;
    	} catch (AssertionFailedError e) {
    	}
    	
    	if (!failed) {
    		fail("Should have failed with audit turned off");
    	}
    }   
}
