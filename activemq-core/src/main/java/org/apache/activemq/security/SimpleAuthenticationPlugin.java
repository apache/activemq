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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.activemq.jaas.GroupPrincipal;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

import java.util.Map;

/**
 * A simple authentication plugin
 * 
 * @org.apache.xbean.XBean element="simpleAuthenticationPlugin"
 *                         description="Provides a simple authentication plugin
 *                         configured with a map of user-passwords and a map of
 *                         user-groups or a list of authentication users"
 * 
 * @version $Revision$
 */
public class SimpleAuthenticationPlugin implements BrokerPlugin {
    private Map userPasswords;
    private Map userGroups;

    public SimpleAuthenticationPlugin() {
    }

    public SimpleAuthenticationPlugin(List users) {
        setUsers(users);
    }

    public Broker installPlugin(Broker broker) {
        return new SimpleAuthenticationBroker(broker, userPasswords, userGroups);
    }

    public Map getUserGroups() {
        return userGroups;
    }

    /**
     * Sets individual users for authentication
     * 
     * @org.apache.xbean.ElementType class="org.apache.activemq.security.AuthenticationUser"
     */
    public void setUsers(List users) {
        userPasswords = new HashMap();
        userGroups = new HashMap();
        for (Iterator it = users.iterator(); it.hasNext();) {
            AuthenticationUser user = (AuthenticationUser)it.next();
            userPasswords.put(user.getUsername(), user.getPassword());
            Set groups = new HashSet();
            StringTokenizer iter = new StringTokenizer(user.getGroups(), ",");
            while (iter.hasMoreTokens()) {
                String name = iter.nextToken().trim();
                groups.add(new GroupPrincipal(name));
            }
            userGroups.put(user.getUsername(), groups);
        }
    }

    /**
     * Sets the groups a user is in. The key is the user name and the value is a
     * Set of groups
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
