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

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.Message;
import javax.jms.MessageFormatRuntimeException;

public interface MessageInterceptorStrategy {
 
    /**
     * When a PolicyEntry is configured with a MessageInterceptorStrategy, the 
     * process method is invoked with the current ProducerBrokerExchange and Message before
     * the message is stored in any destination cache or persistence store.
     * 
     * Implementations may reference data from the ProducerBrokerExchange and may check or
     * modify headers, properties, body or other metadata on the Message.
     * 
     * Any change to the message must adhere to OpenWire and ActiveMQ requirements or risk
     * issues with memory usage, compatibility, and general correct functioning.
     * 
     * Implementations shall not copy, or clone the message.
     * 
     * Implementations may throw a <tt>MessageFormatRuntimeException</tt>
     * that is returned to the client to indicate a message should not be added to the queue.
     * 
     * @param producerBrokerExchange
     * @param message
     * @return
     * @throws MessageFormatRuntimeException
     */
    void process(final ProducerBrokerExchange producerBrokerExchange, final Message message) throws MessageFormatRuntimeException;

}
