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
package org.apache.activemq.command;

import javax.jms.JMSException;
import javax.jms.Topic;

/**
 * @openwire:marshaller
 * @version $Revision: 1.5 $
 */
public class ActiveMQTopic extends ActiveMQDestination implements Topic {

    private static final long serialVersionUID = 7300307405896488588L;
    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.ACTIVEMQ_TOPIC;

    public ActiveMQTopic() {
    }
    
    public ActiveMQTopic(String name) {
        super(name);
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isTopic() {
        return true;
    }

    public String getTopicName() throws JMSException {
        return getPhysicalName();
    }
    
    public byte getDestinationType() {
        return TOPIC_TYPE;
    }

    protected String getQualifiedPrefix() {
        return TOPIC_QUALIFIED_PREFIX;
    }

}
