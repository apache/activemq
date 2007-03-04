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
package org.apache.activemq.broker.region.cursors;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * @version $Revision: 1.3 $
 */
public class AMQStoreCursorDurableTest extends CursorDurableTest{
    
    protected static final Log log = LogFactory.getLog(AMQStoreCursorDurableTest.class);

    

    protected void configureBroker(BrokerService answer) throws Exception{
        AMQPersistenceAdapter adaptor = new AMQPersistenceAdapter("localhost");
        answer.setPersistenceAdapter(adaptor);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(bindAddress);
       
    }
}
