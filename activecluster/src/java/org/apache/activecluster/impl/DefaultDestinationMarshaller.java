/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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



import java.util.Map;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activecluster.DestinationMarshaller;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
 * A simple marshaller for Destinations
 *
 * @version $Revision: 1.5 $
 */
public class DefaultDestinationMarshaller implements DestinationMarshaller {
    private final static Log log = LogFactory.getLog(DefaultDestinationMarshaller.class);

    /**
     * Keep a cache of name to destination mappings for fast lookup.
     */
    private final Map destinations = new ConcurrentHashMap();
    /**
     * The active session used to create a new Destination from a name.
     */
    private final Session session;
    
    /**
     * Create a marshaller for this specific session.
     * @param session the session to use when mapping destinations.
     */
    public DefaultDestinationMarshaller(Session session) {
        this.session = session;
    }
    
    /**
     * Builds a destination from a destinationName
     * @param destinationName 
     *
     * @return the destination to send messages to all members of the cluster
     */
    public Destination getDestination(String destinationName) throws JMSException {
        if (!destinations.containsKey(destinationName)) {
            destinations.put(destinationName, session.createTopic(destinationName));
        }
        return (Destination) destinations.get(destinationName);
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