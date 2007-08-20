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
package org.apache.activemq.memory.buffer;

import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.memory.buffer.MessageBuffer;
import org.apache.activemq.memory.buffer.MessageQueue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @version $Revision: 1.1 $
 */
public abstract class MemoryBufferTestSupport extends TestCase {
    private static final Log LOG = LogFactory.getLog(MemoryBufferTestSupport.class);

    protected MessageBuffer buffer = createMessageBuffer();
    protected MessageQueue qA = buffer.createMessageQueue();
    protected MessageQueue qB = buffer.createMessageQueue();
    protected MessageQueue qC = buffer.createMessageQueue();
    protected int messageCount;

    protected abstract MessageBuffer createMessageBuffer();

    protected void setUp() throws Exception {
        buffer = createMessageBuffer();
        qA = buffer.createMessageQueue();
        qB = buffer.createMessageQueue();
        qC = buffer.createMessageQueue();
    }

    protected void dump() {
        LOG.info("Dumping current state");
        dumpQueue(qA, "A");
        dumpQueue(qB, "B");
        dumpQueue(qC, "C");
    }

    protected void dumpQueue(MessageQueue queue, String name) {
        LOG.info("  " + name + " = " + queue.getList());
    }

    protected ActiveMQMessage createMessage(int size) throws Exception {
        DummyMessage answer = new DummyMessage(size);
        answer.setIntProperty("counter", ++messageCount);
        answer.setJMSMessageID("" + messageCount);
        return answer;
    }

}
