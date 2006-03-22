/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.broker.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;


public interface QueueViewMBean extends DestinationViewMBean {
    
	/**
	 * Retrieve a message from the destination's queue.
	 * 
	 * @param messageId the message id of the message to retreive
	 * @return A CompositeData object which is a JMX version of the messages
	 * @throws OpenDataException
	 */
    public CompositeData getMessage(String messageId) throws OpenDataException;
    
    /**
     * Removes a message from the queue.  If the message has allready been dispatched 
     * to another consumer, the message cannot be delted and this method will return 
     * false.
     * 
     * @param messageId 
     * @return true if the message was found and could be succesfully deleted.
     */
    public boolean removeMessage(String messageId);
    
    /**
     * Emptys out all the messages in the queue.
     */
    public void purge();
    
    /**
     * Copys a given message to another destination.
     * 
     * @param messageId
     * @param destinationName
     * @return true if the message was found and was successfuly copied to the other destination.
     * @throws Exception
     */
    public boolean copyMessageTo(String messageId, String destinationName) throws Exception;

}