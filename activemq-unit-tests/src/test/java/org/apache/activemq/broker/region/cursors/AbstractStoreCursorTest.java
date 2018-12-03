/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.broker.region.cursors;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.MessageId;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertFalse;

public class AbstractStoreCursorTest {

    @Test
    public void testGotToStore() throws Exception  {

        ActiveMQMessage message = new ActiveMQMessage();
        message.setRecievedByDFBridge(true);

        MessageId messageId = new MessageId();
        message.setMessageId(messageId);

        FutureTask<Long> futureTask = new FutureTask<Long>(new Callable<Long>() {
            @Override
            public Long call() {
                return 0l;
            }
        });
        messageId.setFutureOrSequenceLong(futureTask);

        futureTask.cancel(false);

        assertFalse(AbstractStoreCursor.gotToTheStore(message));
    }
}
