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
package org.apache.activemq.security;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.Message;

/**
 * A plugin to allow custom message-level security checks to be performed before
 * a message is consumed.
 * 
 * @version $Revision$
 */
public interface MessageAuthorizationPolicy {

    /**
     * Returns true if the given message is able to be dispatched to the connection
     * performing any user
     * 
     * @return true if the context is allowed to consume the message
     */
    boolean isAllowedToConsume(ConnectionContext context, Message message);

}
