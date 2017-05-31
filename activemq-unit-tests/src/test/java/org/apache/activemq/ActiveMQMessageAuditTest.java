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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ActiveMQMessageAuditTest
 *
 *
 */
public class ActiveMQMessageAuditTest extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(ActiveMQMessageAuditTest.class);

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
        List<String> windowList = list.subList(list.size() -1 -audit.getAuditDepth(), list.size() -1);
        for (String id : windowList) {
            assertTrue("duplicate, id:" + id, audit.isDuplicate(id));
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
            assertFalse(audit.isDuplicate(msg.getMessageId()));
        }
        List<MessageReference> windowList = list.subList(list.size() -1 -audit.getAuditDepth(), list.size() -1);
        for (MessageReference msg : windowList) {
            assertTrue("duplicate msg:" + msg, audit.isDuplicate(msg));
        }
    }

    public void testIsInOrderString() {
        int count = 10000;
        ActiveMQMessageAudit audit = new ActiveMQMessageAudit();
        IdGenerator idGen = new IdGenerator();
        // add to a list
        List<String> list = new ArrayList<String>();
        for (int i = 0; i < count; i++) {
            String id = idGen.generateId();
            if (i==0) {
                assertFalse(audit.isDuplicate(id));
                assertTrue(audit.isInOrder(id));
            }
            if (i > 1 && i%2 != 0) {
                list.add(id);
            }

        }
        for (String id : list) {
            assertFalse(audit.isInOrder(id));
            assertFalse(audit.isDuplicate(id));
        }
    }

    public void testSerialization() throws Exception {
        ActiveMQMessageAuditNoSync audit = new ActiveMQMessageAuditNoSync();

        byte[] bytes =  serialize(audit);
        LOG.debug("Length: " + bytes.length);
        audit = recover(bytes);

        List<MessageReference> list = new ArrayList<MessageReference>();

        for (int j = 0; j < 1000; j++) {
            ProducerId pid = new ProducerId();
            pid.setConnectionId("test");
            pid.setSessionId(0);
            pid.setValue(j);
            LOG.debug("producer " + j);

            for (int i = 0; i < 1000; i++) {
                MessageId id = new MessageId();
                id.setProducerId(pid);
                id.setProducerSequenceId(i);
                ActiveMQMessage msg = new ActiveMQMessage();
                msg.setMessageId(id);
                list.add(msg);
                assertFalse(audit.isDuplicate(msg.getMessageId().toString()));

                if (i % 100 == 0) {
                    bytes = serialize(audit);
                    LOG.debug("Length: " + bytes.length);
                    audit = recover(bytes);
                }

                if (i % 250 == 0) {
                    for (MessageReference message : list) {
                        audit.rollback(message.getMessageId().toString());
                    }
                    list.clear();
                    bytes = serialize(audit);
                    LOG.debug("Length: " + bytes.length);
                    audit = recover(bytes);
                }
            }
        }
    }

    protected byte[] serialize(ActiveMQMessageAuditNoSync audit) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(baos);
        oout.writeObject(audit);
        oout.flush();
        return baos.toByteArray();
    }

    protected ActiveMQMessageAuditNoSync recover(byte[] bytes) throws Exception {
        ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(bytes));
        return (ActiveMQMessageAuditNoSync)objectIn.readObject();
    }
}
