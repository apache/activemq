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
package org.apache.activemq.ra;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * Represents an object which has-a {@link Session} instance and
 * an optional, lazily created {@link MessageProducer} instance
 * which can them be used by a pooling based JMS provider for publishing
 * messages using the session used by the current thread.
 *
 * @version $Revision$
 */
public interface InboundContext {

    /**
     * Returns the current session being used to process a JMS message in the current thread.
     */
    public Session getSession() throws JMSException;

    /**
     * Lazily creates a message producer that can be used to send messages using the
     * same JMS Session which is being used to dispatch messages which minimises the XA
     * overheard of consuming and producing or allows JMS transactions to be used for consuming
     * and producing messages.
     *
     * @return the current message producer or a new one is lazily created, using a null
     *         destination so the destination must be specified on a send() method.
     * @throws javax.jms.JMSException
     */
    public MessageProducer getMessageProducer() throws JMSException;
}
