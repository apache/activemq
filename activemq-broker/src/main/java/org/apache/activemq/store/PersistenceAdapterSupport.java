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
package org.apache.activemq.store;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Used to implement common PersistenceAdapter methods.
 */
public class PersistenceAdapterSupport {

    static public List<SubscriptionInfo> listSubscriptions(PersistenceAdapter pa, String clientId) throws IOException {
        ArrayList<SubscriptionInfo> rc = new ArrayList<SubscriptionInfo>();
        for (ActiveMQDestination destination : pa.getDestinations()) {
            if( destination.isTopic() ) {
                TopicMessageStore store = pa.createTopicMessageStore((ActiveMQTopic) destination);
                for (SubscriptionInfo sub : store.getAllSubscriptions()) {
                    if(clientId==sub.getClientId() || clientId.equals(sub.getClientId()) ) {
                        rc.add(sub);
                    }
                }
            }
        }
        return rc;
    }

}
