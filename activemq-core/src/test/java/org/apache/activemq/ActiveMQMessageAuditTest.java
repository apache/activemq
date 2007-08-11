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

import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.IdGenerator;

/**
 * ActiveMQMessageAuditTest
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ActiveMQMessageAuditTest extends TestCase {


    /**
     * Constructor for ActiveMQMessageAuditTest.
     * 
     * @param name
     */
    public ActiveMQMessageAuditTest(String name) {
        super(name);
    }

    public static void main(String[] args) {
    }

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * test case for isDuplicate
     */
    public void testIsDuplicateString() {
        int count = 10000;
        ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
        IdGenerator idGen = new IdGenerator();
        // add to a list
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
            String id = idGen.generateId();
            list.add(id);
            assertFalse(audit.isDuplicate(id));
        }
        for (String id : list) {
            assertTrue(audit.isDuplicate(id));
        }
    }

    public void testIsDuplicateMessageReference() {
        int count = 10000;
        ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
        // add to a list
        List<MessageReference> list = new ArrayList<MessageReference>();
        for (int i = 0; i < count; i++) {
            ProducerId pid = new ProducerId();
            pid.setConnectionId("test");
            pid.setSessionId(0);
            pid.setValue(1);
            MessageId id = new MessageId();
            id.setProducerId(pid);
            id.setProducerSequenceId(i);
            ActiveMQMessage msg = new ActiveMQMessage();
            msg.setMessageId(id);
            list.add(msg);
            assertFalse(audit.isDuplicateMessageReference(msg));
        }
        for (MessageReference msg : list) {
            assertTrue(audit.isDuplicateMessageReference(msg));
        }
    }
}
