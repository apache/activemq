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

public interface TopicViewMBean extends DestinationViewMBean {
    
	/**
	 * Creates a durable subscription that is subscribed to this topic.
	 * 
	 * @param clientId
	 * @param subscriberName
	 * @throws Exception
	 */
    public void createDurableSubscriber(String clientId, String subscriberName) throws Exception;

    /**
	 * Destroys a durable subscription that had previously subscribed to this topic.
	 * 
	 * @param clientId
	 * @param subscriberName
	 * @throws Exception
	 */
    public void destroyDurableSubscriber(String clientId, String subscriberName) throws Exception;

}