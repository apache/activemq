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
 * 
 **/
package org.activecluster.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.activecluster.DestinationMarshaller;
import org.activecluster.Node;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;


/**
 * A local stub for the state service which sends JMS messages
 * to the cluster
 *
 * @version $Revision: 1.2 $
 */
public class StateServiceStub implements StateService {

    private final Log log = LogFactory.getLog(getClass());

    private Session session;
    private MessageProducer producer;
    private DestinationMarshaller marshaller;

    public StateServiceStub(Session session, MessageProducer producer,DestinationMarshaller marshaller) {
        this.session = session;
        this.producer = producer;
        this.marshaller = marshaller;
    }

    public void keepAlive(Node node) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Sending cluster data message: " + node);
            }

            Message message = session.createObjectMessage(new NodeState(node,marshaller));
            producer.send(message);
        }
        catch (JMSException e) {
            log.error("Could not send JMS message: " + e, e);
        }
    }

    public void shutdown(Node node) {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Sending shutdown message: " + node);
            }

            Message message = session.createObjectMessage(new NodeState(node,marshaller));
            message.setJMSType("shutdown");
            producer.send(message);
        }
        catch (JMSException e) {
            log.error("Could not send JMS message: " + e, e);
        }
    }
}
