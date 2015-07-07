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
package org.apache.activemq.store.memory;

import org.apache.activemq.store.AbstractMessageStoreSizeTest;
import org.apache.activemq.store.MessageStore;

public class MemoryMessageStoreSizeTest extends AbstractMessageStoreSizeTest {

    MemoryMessageStore messageStore;

    @Override
    public void initStore() throws Exception {
        messageStore = new MemoryMessageStore(destination);
        messageStore.start();
    }


    @Override
    public void destroyStore() throws Exception {
        if (messageStore != null) {
            messageStore.stop();
        }
    }

    @Override
    protected MessageStore getMessageStore() {
        return messageStore;
    }

}
