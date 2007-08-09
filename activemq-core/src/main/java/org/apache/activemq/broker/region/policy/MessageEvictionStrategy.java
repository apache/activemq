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
package org.apache.activemq.broker.region.policy;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.activemq.broker.region.MessageReference;

/**
 * A strategy for evicting messages from slow consumers.
 * 
 * @version $Revision$
 */
public interface MessageEvictionStrategy {

    /**
     * Find the message reference in the given list with oldest messages at the front and newer messages at the end
     * 
     * @return the message that has been evicted.
     * @throws IOException if an exception occurs such as reading a message content (but should not ever happen
     * as usually all the messages will be in RAM when this method is called).
     */
    MessageReference[] evictMessages(LinkedList messages) throws IOException;

    /**
     * REturns the high water mark on which we will eagerly evict expired messages from RAM
     */
    int getEvictExpiredMessagesHighWatermark();

}
