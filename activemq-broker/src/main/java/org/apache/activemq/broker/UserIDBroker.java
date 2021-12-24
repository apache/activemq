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
package org.apache.activemq.broker;

import java.util.Set;
import org.apache.activemq.command.Message;
import org.apache.activemq.jaas.UserPrincipal;
import org.apache.activemq.security.SecurityContext;

/**
 * This broker filter will append the producer's user ID into the JMSXUserID header
 * to allow folks to know reliably who the user was who produced a message.
 * Note that you cannot trust the client, especially if working over the internet
 * as they can spoof headers to be anything they like.
 * 
 * 
 */
public class UserIDBroker extends BrokerFilter {
    boolean useAuthenticatePrincipal = false;
    public UserIDBroker(Broker next) {
        super(next);
    }

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        final ConnectionContext context = producerExchange.getConnectionContext();
        if(context.isNetworkConnection() && messageSend.getUserID() != null) {
            super.send(producerExchange, messageSend);
            return;
        }
        String userID = context.getUserName();
        if (isUseAuthenticatePrincipal()) {
            SecurityContext securityContext = context.getSecurityContext();
            if (securityContext != null) {
                Set<?> principals = securityContext.getPrincipals();
                if (principals != null) {
                    for (Object candidate : principals) {
                        if (candidate instanceof UserPrincipal) {
                            userID = ((UserPrincipal)candidate).getName();
                            break;
                        }
                    }
                }
            }
        }
        messageSend.setUserID(userID);
        super.send(producerExchange, messageSend);
    }


    public boolean isUseAuthenticatePrincipal() {
        return useAuthenticatePrincipal;
    }

    public void setUseAuthenticatePrincipal(boolean useAuthenticatePrincipal) {
        this.useAuthenticatePrincipal = useAuthenticatePrincipal;
    }
}
