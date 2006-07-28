/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.systest;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import java.util.List;

/**
 * Represents a list of messages
 * 
 * @version $Revision: 1.1 $
 */
public interface MessageList extends Agent {

    int getSize();

    void assertMessagesCorrect(List list) throws JMSException;

    /**
     * Sends the messages on the given producer
     * 
     * @throws JMSException
     */
    void sendMessages(Session session, MessageProducer producer) throws JMSException;

    /**
     * Sends a percent of the messages to the given producer
     * @throws JMSException 
     */
    void sendMessages(Session session, MessageProducer producer, int percent) throws JMSException;

}
