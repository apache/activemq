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

/**
 * An authorization plugin where each operation on a destination is checked
 * against an authorizationMap
 * 
 * @org.apache.xbean.XBean
 * 
 * @version $Revision$
 */
public class AuthorizationPlugin implements BrokerPlugin {

    private AuthorizationMap authorizationMap;

    public AuthorizationPlugin() {
    }

    public AuthorizationPlugin(AuthorizationMap authorizationMap) {
        this.authorizationMap = authorizationMap;
    }

    public Broker installPlugin(Broker broker) {
        if (authorizationMap == null) {
            throw new IllegalArgumentException("You must configure an 'authorizationMap'");
        }
        return new AuthorizationBroker(broker, authorizationMap);
    }

    public AuthorizationMap getAuthorizationMap() {
        return authorizationMap;
    }

    public void setAuthorizationMap(AuthorizationMap authorizationMap) {
        this.authorizationMap = authorizationMap;
    }

}
