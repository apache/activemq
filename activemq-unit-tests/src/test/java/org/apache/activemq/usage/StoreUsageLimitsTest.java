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

package org.apache.activemq.usage;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;

public class StoreUsageLimitsTest extends EmbeddedBrokerTestSupport {

    final int WAIT_TIME_MILLS = 20 * 1000;
    private static final String limitsLogLevel = "warn";

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.getSystemUsage().getMemoryUsage().setLimit(Long.MAX_VALUE);
        broker.getSystemUsage().setCheckLimitsLogLevel(limitsLogLevel);
        broker.deleteAllMessages();
        return broker;
    }

    @Override
    protected boolean isPersistent() {
        return true;
    }

    public void testCheckLimitsLogLevel() throws Exception {

        File file = new File("target/activemq-test.log");
        if (!file.exists()) {
            fail("target/activemq-test.log was not created.");
        }

        BufferedReader br = null;
        boolean foundUsage = false;

        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.contains(new String(Long.toString(Long.MAX_VALUE / (1024 * 1024)))) && line.contains(limitsLogLevel.toUpperCase())) {
                    foundUsage = true;
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            br.close();
        }

        if (!foundUsage)
            fail("checkLimitsLogLevel message did not write to log target/activemq-test.log");
    }
}
