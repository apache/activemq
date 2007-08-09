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
package org.apache.activemq;

import javax.jms.MessageConsumer;

/**
 * An extended JMS interface that adds the ability to be notified when a 
 * message is available for consumption using the receive*() methods
 * which is useful in Ajax style subscription models.
 * 
 * @version $Revision: 1.1 $
 */
public interface MessageAvailableConsumer extends MessageConsumer {

    /**
     * Sets the listener used to notify synchronous consumers that there is a message
     * available so that the {@link MessageConsumer#receiveNoWait()} can be called.
     */
    void setAvailableListener(MessageAvailableListener availableListener);

    /**
     * Gets the listener used to notify synchronous consumers that there is a message
     * available so that the {@link MessageConsumer#receiveNoWait()} can be called.
     */
    MessageAvailableListener getAvailableListener();
}
