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
package org.apache.activemq.security;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.command.ActiveMQDestination;

/**
 * Used to cache up authorizations so that subsequent requests are faster.
 * 
 * @version $Revision$
 */
public abstract class SecurityContext {

    public static final SecurityContext BROKER_SECURITY_CONTEXT = new SecurityContext("ActiveMQBroker") {
        @Override
        public boolean isBrokerContext() {
            return true;
        }

        @SuppressWarnings("unchecked")
        public Set<?> getPrincipals() {
            return Collections.EMPTY_SET;
        }
    };

    final String userName;

    final ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination> authorizedReadDests = new ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination>();
    final ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination> authorizedWriteDests = new ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination>();

    public SecurityContext(String userName) {
        this.userName = userName;
    }

    public boolean isInOneOf(Set<?> allowedPrincipals) {
        HashSet<?> set = new HashSet<Object>(getPrincipals());
        set.retainAll(allowedPrincipals);
        return set.size() > 0;
    }

    public abstract Set<?> getPrincipals();

    public String getUserName() {
        return userName;
    }

    public ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination> getAuthorizedReadDests() {
        return authorizedReadDests;
    }

    public ConcurrentHashMap<ActiveMQDestination, ActiveMQDestination> getAuthorizedWriteDests() {
        return authorizedWriteDests;
    }

    public boolean isBrokerContext() {
        return false;
    }
}
