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
package org.apache.activemq.openwire.v3;

import org.apache.activemq.command.BrokerId;
import org.apache.activemq.command.ConsumerInfo;

/**
 * Test case for the OpenWire marshalling for ConsumerInfo NOTE!: This file is
 * auto generated - do not modify! if you need to make a change, please see the
 * modify the groovy scripts in the under src/gram/script and then use maven
 * openwire:generate to regenerate this file.
 * 
 * @version $Revision: $
 */
public class ConsumerInfoTest extends BaseCommandTestSupport {

    public static final ConsumerInfoTest SINGLETON = new ConsumerInfoTest();

    public Object createObject() throws Exception {
        ConsumerInfo info = new ConsumerInfo();
        populateObject(info);
        return info;
    }

    protected void populateObject(Object object) throws Exception {
        super.populateObject(object);
        ConsumerInfo info = (ConsumerInfo)object;

        info.setConsumerId(createConsumerId("ConsumerId:1"));
        info.setBrowser(true);
        info.setDestination(createActiveMQDestination("Destination:2"));
        info.setPrefetchSize(1);
        info.setMaximumPendingMessageLimit(2);
        info.setDispatchAsync(false);
        info.setSelector("Selector:3");
        info.setSubscriptionName("SubscriptionName:4");
        info.setNoLocal(true);
        info.setExclusive(false);
        info.setRetroactive(true);
        info.setPriority((byte)1);
        {
            BrokerId value[] = new BrokerId[2];
            for (int i = 0; i < 2; i++) {
                value[i] = createBrokerId("BrokerPath:5");
            }
            info.setBrokerPath(value);
        }
        info.setAdditionalPredicate(createBooleanExpression("AdditionalPredicate:6"));
        info.setNetworkSubscription(false);
        info.setOptimizedAcknowledge(true);
        info.setNoRangeAcks(false);
    }
}
