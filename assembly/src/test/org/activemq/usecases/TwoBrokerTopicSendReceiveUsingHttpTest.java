/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.usecases;

import org.activemq.ActiveMQConnectionFactory;

import javax.jms.JMSException;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class TwoBrokerTopicSendReceiveUsingHttpTest extends TwoBrokerTopicSendReceiveTest {

    protected ActiveMQConnectionFactory createReceiverConnectionFactory() throws JMSException {
        return createConnectionFactory("org/activemq/usecases/receiver-http.xml", "receiver", "vm://receiver");
    }

    protected ActiveMQConnectionFactory createSenderConnectionFactory() throws JMSException {
        return createConnectionFactory("org/activemq/usecases/sender-http.xml", "sender", "vm://sender");
    }
}
