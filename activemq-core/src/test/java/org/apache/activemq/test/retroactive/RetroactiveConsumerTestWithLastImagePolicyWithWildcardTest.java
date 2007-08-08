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
package org.apache.activemq.test.retroactive;

import org.apache.activemq.command.ActiveMQTopic;

import javax.jms.MessageProducer;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;

/**
 *
 * @version $Revision$
 */
public class RetroactiveConsumerTestWithLastImagePolicyWithWildcardTest extends RetroactiveConsumerTestWithSimpleMessageListTest {
    private int counter = 1;
    
    protected void sendMessage(MessageProducer producer, TextMessage message) throws JMSException {
        ActiveMQTopic topic = new ActiveMQTopic(destination.toString() + "." + (counter++));
//        System.out.println("Sending to destination: " + topic);
        producer.send(topic, message);
    }

    protected MessageProducer createProducer() throws JMSException {
        return session.createProducer(null);
    }

    protected MessageConsumer createConsumer() throws JMSException {
        return session.createConsumer( new ActiveMQTopic(destination.toString() + ".>"));
    }

    protected String getBrokerXml() {
        return "org/apache/activemq/test/retroactive/activemq-lastimage-policy.xml";
    }
}
