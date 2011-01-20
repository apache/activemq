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
package org.apache.activemq;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * The StreamConnection interface allows you to send and receive data from a
 * Destination in using standard java InputStream and OutputStream objects. It's
 * best use case is to send and receive large amounts of data that would be to
 * large to hold in a single JMS message.
 * 
 * @version $Revision$
 */
public interface StreamConnection extends Connection {

    InputStream createInputStream(Destination dest) throws JMSException;

    InputStream createInputStream(Destination dest, String messageSelector) throws JMSException;

    InputStream createInputStream(Destination dest, String messageSelector, boolean noLocal) throws JMSException;

    InputStream createInputStream(Destination dest, String messageSelector, boolean noLocal, long timeout) throws JMSException;

    InputStream createDurableInputStream(Topic dest, String name) throws JMSException;

    InputStream createDurableInputStream(Topic dest, String name, String messageSelector) throws JMSException;

    InputStream createDurableInputStream(Topic dest, String name, String messageSelector, boolean noLocal) throws JMSException;
    
    InputStream createDurableInputStream(Topic dest, String name, String messageSelector, boolean noLocal, long timeout) throws JMSException;

    OutputStream createOutputStream(Destination dest) throws JMSException;

    OutputStream createOutputStream(Destination dest, Map<String, Object> streamProperties, int deliveryMode, int priority, long timeToLive) throws JMSException;

    /**
     * Unsubscribes a durable subscription that has been created by a client.
     * <P>
     * This method deletes the state being maintained on behalf of the
     * subscriber by its provider.
     * <P>
     * It is erroneous for a client to delete a durable subscription while there
     * is an active <CODE>MessageConsumer </CODE> or
     * <CODE>TopicSubscriber</CODE> for the subscription, or while a consumed
     * message is part of a pending transaction or has not been acknowledged in
     * the session.
     * 
     * @param name the name used to identify this subscription
     * @throws JMSException if the session fails to unsubscribe to the durable
     *                 subscription due to some internal error.
     * @throws InvalidDestinationException if an invalid subscription name is
     *                 specified.
     * @since 1.1
     */
    void unsubscribe(String name) throws JMSException;
}
