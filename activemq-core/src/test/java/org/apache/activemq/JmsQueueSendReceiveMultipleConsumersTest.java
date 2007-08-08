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
package org.apache.activemq;
import javax.jms.MessageConsumer;


/**
 * @version $Revision: 1.2 $
 */
public class JmsQueueSendReceiveMultipleConsumersTest extends JmsQueueSendReceiveTest{
    MessageConsumer consumer1;
    MessageConsumer consumer2;
    
    protected void setUp() throws Exception {
        messageCount = 5000;
        super.setUp();
       
        consumer1 = createConsumer();
        consumer1.setMessageListener(this);
        consumer2 = createConsumer();
        consumer2.setMessageListener(this);
        
    }
}
