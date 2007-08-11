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

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.systest.DestinationFactory;

import javax.jms.Destination;
import javax.jms.JMSException;

/**
 * 
 * @version $Revision: 1.1 $
 */
public class DestinationFactoryImpl implements DestinationFactory {

    public Destination createDestination(String physicalName, int destinationType) throws JMSException {
        switch (destinationType) {

        case TOPIC:
            return new ActiveMQTopic(physicalName);

        case QUEUE:
            return new ActiveMQQueue(physicalName);
            
        case TEMPORARY_QUEUE:
            return new ActiveMQTempQueue(physicalName);

        case TEMPORARY_TOPIC:
            return new ActiveMQTempTopic(physicalName);

        default:
            throw new JMSException("Invalid destinationType: " + destinationType);
        }
    }

}
