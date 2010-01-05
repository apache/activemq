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
package org.apache.activemq.store.temp;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.kahadb.util.IOHelper;

public class TempStoreTest extends TestCase {
    protected TempStore ts;
    protected final LinkedList<Message> testList = new LinkedList<Message>();
    protected List<Message> list;

    public void testSize() throws Exception {

        this.list.addAll(testList);
        
        assertEquals(this.list.size(), this.testList.size());

    }

    public void testAddFirst() throws Exception {
        this.list.addAll(testList);
        assertEquals(this.list.size(), this.testList.size());
        Message first = createMessage(10000);
        this.list.add(0, first);
        assertEquals(first, this.list.get(0));
        assertEquals(this.list.size(), this.testList.size() + 1);
    }
    
    public void testAddLast() throws Exception {
        this.list.addAll(testList);
        assertEquals(this.list.size(), this.testList.size());
        Message last = createMessage(10000);
        this.list.add(last);
        assertEquals(last, this.list.get(this.list.size()-1));
        assertEquals(this.list.size(), this.testList.size() + 1);
    }
    
    public void testRemoveFirst() throws Exception {
        this.list.addAll(testList);
        assertEquals(testList.get(0), this.list.remove(0));
        assertEquals(this.list.size(), testList.size() - 1);
        System.err.println(this.list.get(0).getMessageId());
        for (int i =0; i < testList.size();i++) {
            System.err.println(testList.get(i).getMessageId());
        }
        for (int i = 1; i < testList.size(); i++) {
            assertEquals(testList.get(i), this.list.get(i - 1));
        }
    }

    private Message createMessage(int seq) {
        ActiveMQMessage message = new ActiveMQMessage();
        message.setCommandId((short) 1);
        message.setDestination(new ActiveMQQueue("queue"));

        message.setMessageId(new MessageId("c1:1:1", seq));
        return message;

    }

    protected void setUp() throws Exception {
        super.setUp();
        this.ts = new TempStore();
        File directory = new File("target/test");
        this.ts.setDirectory(directory);
        IOHelper.mkdirs(directory);
        IOHelper.deleteChildren(directory);

        ts.start();
        for (int i = 0; i < 10; i++) {

            Message msg = createMessage(i);
            testList.add(msg);
        }
        this.list = ts.getList(getName());
    }

    protected void tearDown() throws Exception {
        testList.clear();
        if (ts != null) {
            ts.stop();
        }
    }
}
