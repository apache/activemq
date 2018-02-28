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

import junit.framework.Test;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;

public class JdbcXARecoveryBrokerNoCacheTest extends JdbcXARecoveryBrokerTest {

    @Override
    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policyEntry = super.getDefaultPolicy();
        policyEntry.setUseCache(false);
        policyEntry.setMaxPageSize(5);
        return policyEntry;
    }

    public static Test suite() {
        return suite(JdbcXARecoveryBrokerNoCacheTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue("testNoCache");
    }
}
