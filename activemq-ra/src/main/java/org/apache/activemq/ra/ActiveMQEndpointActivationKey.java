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
package org.apache.activemq.ra;

import javax.resource.spi.endpoint.MessageEndpointFactory;


public class ActiveMQEndpointActivationKey {
    final private MessageEndpointFactory messageEndpointFactory;
    final private ActiveMQActivationSpec activationSpec;

    /**
     * @return Returns the activationSpec.
     */
    public ActiveMQActivationSpec getActivationSpec() {
        return activationSpec;
    }

    /**
     * @return Returns the messageEndpointFactory.
     */
    public MessageEndpointFactory getMessageEndpointFactory() {
        return messageEndpointFactory;
    }

    /**
     * For testing
     */ 
    ActiveMQEndpointActivationKey() {
        this(null, null);
    }

    /**
     * @param messageEndpointFactory
     * @param activationSpec
     */
    public ActiveMQEndpointActivationKey(MessageEndpointFactory messageEndpointFactory, ActiveMQActivationSpec activationSpec) {
        this.messageEndpointFactory = messageEndpointFactory;
        this.activationSpec = activationSpec;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    public int hashCode() {
        return messageEndpointFactory.hashCode() ^ activationSpec.hashCode();
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        ActiveMQEndpointActivationKey o = (ActiveMQEndpointActivationKey) obj;

        //Per the 12.4.9 spec: 
        //   MessageEndpointFactory does not implement equals()
        //   ActivationSpec does not implement equals()
        return o.activationSpec == activationSpec && o.messageEndpointFactory == messageEndpointFactory;
    }
}