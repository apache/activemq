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

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;


/**
 * A JMS MessageListener which processes inbound messages and
 * applies them to a StateService
 *
 * @version $Revision: 1.2 $
 */
public class StateConsumer implements MessageListener {

    private final static Log log = LogFactory.getLog(StateConsumer.class);

    private StateService stateService;
    private DestinationMarshaller marshaller;

    public StateConsumer(StateService stateService,DestinationMarshaller marshaller) {
        if (stateService == null) {
            throw new IllegalArgumentException("Must specify a valid StateService implementation");
        }
        this.stateService = stateService;
        this.marshaller = marshaller;
    }

    public void onMessage(Message message) {
        if (log.isDebugEnabled()) {
            log.debug("Received cluster data message!: " + message);
        }

        if (message instanceof ObjectMessage) {
            ObjectMessage objectMessage = (ObjectMessage) message;
            try {
                NodeState nodeState = (NodeState) objectMessage.getObject();
                Node node = new NodeImpl(nodeState,marshaller);
                String type = objectMessage.getJMSType();
                if (type != null && type.equals("shutdown")) {
                    stateService.shutdown(node);
                }
                else {
                    stateService.keepAlive(node);
                }
            }
            catch (Exception e) {
                log.error("Could not extract node from message: " + e + ". Message: " + message, e);
            }
        }
        else {
            log.warn("Ignoring message: " + message);
        }
    }
}
