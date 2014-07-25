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

import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DeadLetterStrategy} where each destination has its own individual
 * DLQ using the subject naming hierarchy.
 *
 * @org.apache.xbean.XBean element="discarding" description="Dead Letter Strategy that discards all messages"
 *
 */
public class DiscardingDeadLetterStrategy extends SharedDeadLetterStrategy {

    private static final Logger LOG = LoggerFactory.getLogger(DiscardingDeadLetterStrategy.class);

    @Override
    public boolean isSendToDeadLetterQueue(Message message) {
        boolean result = false;
        LOG.debug("Discarding message sent to DLQ: {}, dest: {}", message.getMessageId(), message.getDestination());
        return result;
    }
}
