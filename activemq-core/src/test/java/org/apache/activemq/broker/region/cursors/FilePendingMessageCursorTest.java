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
package org.apache.activemq.broker.region.cursors;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.QueueMessageReference;
import org.apache.activemq.store.kahadb.plist.PList;
import org.apache.activemq.usage.SystemUsage;
import org.apache.kahadb.util.ByteSequence;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilePendingMessageCursorTest {

    BrokerService brokerService;
    FilePendingMessageCursor underTest;

    @Before
    public void createBrokerWithTempStoreLimit() throws Exception {
        brokerService = new BrokerService();
        SystemUsage usage = brokerService.getSystemUsage();
        usage.getTempUsage().setLimit(1025*1024*15);

        // put something in the temp store to on demand initialise it
        PList dud = brokerService.getTempDataStore().getPList("dud");
        dud.addFirst("A", new ByteSequence("A".getBytes()));
    }

    @Test
    public void testAddToEmptyCursorWhenTempStoreIsFull() throws Exception {

        SystemUsage usage = brokerService.getSystemUsage();
        assertTrue("temp store is full: %" + usage.getTempUsage().getPercentUsage(), usage.getTempUsage().isFull());

        underTest = new FilePendingMessageCursor(brokerService.getBroker(), "test", false);
        underTest.setSystemUsage(usage);

        // ok to add
        underTest.addMessageLast(QueueMessageReference.NULL_MESSAGE);

        assertFalse("cursor is not full", underTest.isFull());
    }
}
