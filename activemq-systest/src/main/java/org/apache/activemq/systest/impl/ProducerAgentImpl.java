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
package org.apache.activemq.systest.impl;

import org.apache.activemq.systest.AgentStopper;
import org.apache.activemq.systest.MessageList;
import org.apache.activemq.systest.ProducerAgent;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;

/**
 * A simple in JVM implementation of a {@link ProducerAgent}
 * 
 * @version $Revision: 1.1 $
 */
public class ProducerAgentImpl extends JmsClientSupport implements ProducerAgent {

    private MessageProducer producer;
    private boolean persistent = true;

    public void start() throws Exception {
        super.start();
        producer = createProducer();
    }

    public void sendMessages(MessageList messageList) throws JMSException {
        System.out.println("About to send: " + messageList.getSize() + " message(s) to destination: " + getDestination());
        messageList.sendMessages(getSession(), producer);
        if (isTransacted()) {
            getSession().commit();
        }
        System.out.println("Sent: " + messageList.getSize() + " message(s) to destination: " + getDestination());
    }
    
    public void sendMessages(MessageList messageList, int percent) throws JMSException {
        System.out.println("About to send: " + percent + " % of the message(s) to destination: " + getDestination());
        messageList.sendMessages(getSession(), producer, percent);
        if (isTransacted()) {
            getSession().commit();
        }
        System.out.println("Sent: " + percent + " % of the message(s) to destination: " + getDestination());
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public void stop(AgentStopper stopper) {
        if (producer != null) {
            try {
                producer.close();
            }
            catch (JMSException e) {
                stopper.onException(this, e);
            }
            finally {
                producer = null;
            }
        }
        super.stop(stopper);
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected MessageProducer createProducer() throws JMSException {
        MessageProducer answer = getSession().createProducer(getDestination());
        answer.setDeliveryMode(isPersistent() ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
        return answer;
    }

}
