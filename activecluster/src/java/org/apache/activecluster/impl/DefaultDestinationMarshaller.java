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

package org.apache.activecluster.impl;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activecluster.DestinationMarshaller;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple marshaller for Destinations
 *
 * @version $Revision: 1.5 $
 */
public class DefaultDestinationMarshaller implements DestinationMarshaller {
    private final static Log log = LogFactory.getLog(DefaultDestinationMarshaller.class);
    /**
     * Builds a destination from a destinationName
     * @param destinationName 
     *
     * @return the destination to send messages to all members of the cluster
     */
    public Destination getDestination(String destinationName){
        return new ActiveMQTopic(destinationName);
    }

    /**
     * Gets a destination's physical name
     * @param destination
     * @return the destination's physical name
     */
    public String getDestinationName(Destination destination){
        String result = null;
        if (destination != null){
            if (destination instanceof Topic){
                Topic topic = (Topic) destination;
                try{
                    result = topic.getTopicName();
                }catch(JMSException e){
                    log.error("Failed to get topic name for " + destination,e);
                }
            }else{
                Queue queue = (Queue) destination;
                try{
                    result = queue.getQueueName();
                }catch(JMSException e){
                    log.error("Failed to get queue name for " + destination,e);
                }
            }
        }
        return result;
    }
}