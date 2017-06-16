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

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BrokerServiceTest {

    static class Hook implements Runnable {

        boolean invoked = false;

        @Override
        public void run() {
            invoked = true;
        }
    }

    @Test
    public void removedPreShutdownHooksShouldNotBeInvokedWhenStopping() throws Exception {
        final BrokerService brokerService = new BrokerService();

        final Hook hook = new Hook();
        brokerService.addPreShutdownHook(hook);
        brokerService.removePreShutdownHook(hook);

        brokerService.stop();

        assertFalse("Removed pre-shutdown hook should not have been invoked", hook.invoked);
    }

    @Test
    public void shouldInvokePreShutdownHooksBeforeStopping() throws Exception {
        final BrokerService brokerService = new BrokerService();

        final Hook hook = new Hook();
        brokerService.addPreShutdownHook(hook);

        brokerService.stop();

        assertTrue("Pre-shutdown hook should have been invoked", hook.invoked);
    }
}
