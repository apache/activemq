/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.management;

import javax.jms.Message;

/**
 * A simple interface to allow statistics gathering to be easily switched out
 * for performance reasons.
 *
 * @version $Revision: 1.2 $
 */
public interface JMSDestinationStats {
    /**
     * On startup sets the pending message count
     *
     * @param count
     */
    public void setPendingMessageCountOnStartup(long count);

    /**
     * On a message send to this destination, update the producing stats
     *
     * @param message
     */
    public void onMessageSend(Message message);

    /**
     * On a consume from this destination, updates the consumed states
     */
    public void onMessageAck();
}
