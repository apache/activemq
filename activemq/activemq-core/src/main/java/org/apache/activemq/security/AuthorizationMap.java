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

import org.apache.activemq.command.ActiveMQDestination;

import java.util.Set;

/**
 *
 * @version $Revision$
 */
public interface AuthorizationMap {

    /**
     * Returns the set of all ACLs capable of administering the given destination
     */
    Set getAdminACLs(ActiveMQDestination destination);

    /**
     * Returns the set of all ACLs capable of reading (consuming from) the given destination
     */
    Set getReadACLs(ActiveMQDestination destination);

    /**
     * Returns the set of all ACLs capable of writing to the given destination
     */
    Set getWriteACLs(ActiveMQDestination destination);

}
