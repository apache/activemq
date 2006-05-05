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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

import java.util.Map;

/**
 * A simple authentication plugin
 *  
 * @org.apache.xbean.XBean element="simpleAuthenticationPlugin" description="Provides a simple authentication
 * plugin configured with a map of user-passwords and a map of user-groups"
 * 
 * @version $Revision$
 */
public class SimpleAuthenticationPlugin implements BrokerPlugin {
    private Map userPasswords;
    private Map userGroups;

    public Broker installPlugin(Broker broker) {
        return new SimpleAuthenticationBroker(broker, userPasswords, userGroups);
    }

    public Map getUserGroups() {
        return userGroups;
    }

    /**
     * Sets the groups a user is in. The key is the user name and the value is a Set of groups
     */
    public void setUserGroups(Map userGroups) {
        this.userGroups = userGroups;
    }

    public Map getUserPasswords() {
        return userPasswords;
    }

    /**
     * Sets the map indexed by user name with the value the password
     */
    public void setUserPasswords(Map userPasswords) {
        this.userPasswords = userPasswords;
    }

}
