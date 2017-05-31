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
package org.apache.activemq.broker.ft;

import java.util.concurrent.TimeUnit;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.LeaseLockerIOExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DbRestartJDBCQueueMasterSlaveLeaseQuiesceTest extends DbRestartJDBCQueueMasterSlaveLeaseTest {
    private static final transient Logger LOG = LoggerFactory.getLogger(DbRestartJDBCQueueMasterSlaveLeaseQuiesceTest.class);

    private long restartDelay = 2000;

    @Override
    protected void configureBroker(BrokerService brokerService) {
        // master and slave survive db restart and retain master/slave status
        LeaseLockerIOExceptionHandler stopConnectors = new LeaseLockerIOExceptionHandler();
        brokerService.setIoExceptionHandler(stopConnectors);
    }

    @Override
    protected void delayTillRestartRequired() {
        if (restartDelay > 2000) {
            LOG.info("delay for more than lease quantum. While Db is offline, master should stay alive but could loose lease");
        } else {
            LOG.info("delay for less than lease quantum. While Db is offline, master should stay alive");
        }
        try {
            TimeUnit.MILLISECONDS.sleep(restartDelay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void verifyExpectedBroker(int inflightMessageCount) {
        if (inflightMessageCount == 0  || (inflightMessageCount == failureCount + 10 && restartDelay <= 500)) {
            assertEquals("connected to master", master.getBrokerName(), ((ActiveMQConnection)sendConnection).getBrokerName());
        } else {
            // lease expired while DB was offline, either or master/slave can grab it so assert is not deterministic
            // but we still need to validate sent == received
        }
    }

    @Override
    public void setUp() throws Exception {
        restartDelay = 2000;
        super.setUp();
    }

    public void testSendReceiveWithLeaseExpiry() throws Exception {
        restartDelay = 10000;
        testSendReceive();
    }

    // ignore this test case
    public void testAdvisory() throws Exception {}

   @Override
   public void testSendReceive() throws Exception {
       // Ignore this test for now, see AMQ-4975
   }
}
