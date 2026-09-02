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
package org.apache.activemq.broker.region;

import static org.junit.Assert.assertEquals;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

/**
 * [AMQ-9692] Direct coverage of the package-private BaseDestination.canGcConsumer
 * predicate, which decides whether a single subscription may be ignored by
 * destination gc:
 *
 *   (gcWithNetworkConsumers AND network) OR
 *   (gcWithOnlyWildcardConsumers AND wildcard AND NOT durable)
 *
 * The table covers every combination of the two policy flags and the three
 * subscription traits. Notable rows it pins:
 * - a durable subscription is never gc-eligible via the wildcard branch,
 *   regardless of flags
 * - the network branch predates the durable exclusion and intentionally does
 *   not consult isDurable() - a durable network (bridge demand) subscription
 *   remains gc-eligible under gcWithNetworkConsumers, as before this feature
 *
 * gcNC = gcWithNetworkConsumers, gcWC = gcWithOnlyWildcardConsumers
 * net = network subscription, wild = wildcard, dur = durable
 */
@RunWith(Parameterized.class)
public class DestinationCanGcConsumerTest {

    private static BrokerService brokerService;
    private static BaseDestination baseDestination;

    @BeforeClass
    public static void beforeClass() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.start();
        brokerService.waitUntilStarted();

        var queue = new ActiveMQQueue("amq.gc.canGcConsumer");
        brokerService.getBroker().addDestination(
                brokerService.getAdminConnectionContext(), queue, false);
        baseDestination = (BaseDestination) brokerService.getDestination(queue);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Parameterized.Parameters(name = "gcNC={0} gcWC={1} net={2} wild={3} dur={4} exp={5}")
    public static Collection<Object[]> data() {
        var truthTable = new Object[][] {
                // Plain app subscription - never gc-eligible
                { false, false, false, false, false, false },
                { false, true,  false, false, false, false },
                { true,  false, false, false, false, false },
                { true,  true,  false, false, false, false },

                // Network subscription - gc-eligible iff gcNC
                { false, false, true,  false, false, false },
                { false, true,  true,  false, false, false },
                { true,  false, true,  false, false, true  },
                { true,  true,  true,  false, false, true  },

                // Wildcard subscription - gc-eligible iff gcWC
                { false, false, false, true,  false, false },
                { false, true,  false, true,  false, true  },
                { true,  false, false, true,  false, false },
                { true,  true,  false, true,  false, true  },

                // Durable (non-wildcard) subscription - never gc-eligible
                { false, false, false, false, true,  false },
                { false, true,  false, false, true,  false },
                { true,  false, false, false, true,  false },
                { true,  true,  false, false, true,  false },

                // Durable wildcard subscription - the durable exclusion blocks
                // the wildcard branch under every flag combination
                { false, false, false, true,  true,  false },
                { false, true,  false, true,  true,  false },
                { true,  false, false, true,  true,  false },
                { true,  true,  false, true,  true,  false },

                // Network + wildcard - eligible via either enabled branch
                { false, false, true,  true,  false, false },
                { false, true,  true,  true,  false, true  },
                { true,  false, true,  true,  false, true  },
                { true,  true,  true,  true,  false, true  },

                // Network + durable - the network branch does not consult
                // isDurable() (pre-existing gcWithNetworkConsumers semantics)
                { false, false, true,  false, true,  false },
                { false, true,  true,  false, true,  false },
                { true,  false, true,  false, true,  true  },
                { true,  true,  true,  false, true,  true  },

                // Network + durable wildcard - eligible only via the network
                // branch; the wildcard branch stays blocked by the durable
                { false, false, true,  true,  true,  false },
                { false, true,  true,  true,  true,  false },
                { true,  false, true,  true,  true,  true  },
                { true,  true,  true,  true,  true,  true  }
        };
        var params = new ArrayList<Object[]>();
        for (var row : truthTable) {
            params.add(row);
        }
        return params;
    }

    private final boolean gcWithNetworkConsumersEnabled;
    private final boolean gcWithOnlyWildcardConsumersEnabled;
    private final boolean networkSubscription;
    private final boolean wildcardSubscription;
    private final boolean durableSubscription;
    private final boolean canGcExpected;

    public DestinationCanGcConsumerTest(boolean gcWithNetworkConsumersEnabled, boolean gcWithOnlyWildcardConsumersEnabled, boolean networkSubscription, boolean wildcardSubscription, boolean durableSubscription, boolean canGcExpected) {
        this.gcWithNetworkConsumersEnabled = gcWithNetworkConsumersEnabled;
        this.gcWithOnlyWildcardConsumersEnabled = gcWithOnlyWildcardConsumersEnabled;
        this.networkSubscription = networkSubscription;
        this.wildcardSubscription = wildcardSubscription;
        this.durableSubscription = durableSubscription;
        this.canGcExpected = canGcExpected;
    }

    @Test
    public void testCanGcConsumer() throws Exception {
        baseDestination.setGcWithNetworkConsumers(gcWithNetworkConsumersEnabled);
        baseDestination.setGcWithOnlyWildcardConsumers(gcWithOnlyWildcardConsumersEnabled);

        var subscription = new MockSubscription(baseDestination.getActiveMQDestination(),
                networkSubscription, wildcardSubscription, durableSubscription);

        assertEquals(canGcExpected, baseDestination.canGcConsumer.test(subscription));
    }

    static class MockConsumerInfo extends ConsumerInfo {

        private final boolean networkSubscription;
        private final boolean durableSubscription;

        public MockConsumerInfo(ActiveMQDestination activemqDestination, boolean networkSubscription, boolean durableSubscription) {
            setDestination(activemqDestination);
            this.networkSubscription = networkSubscription;
            this.durableSubscription = durableSubscription;
        }

        @Override
        public boolean isNetworkSubscription() {
            return this.networkSubscription;
        }

        @Override
        public boolean isDurable() {
            return this.durableSubscription;
        }
    }

    static class MockSubscription extends QueueSubscription {

        private final boolean wildcardSubscription;

        public MockSubscription(ActiveMQDestination activemqDestination, boolean networkSubscription, boolean wildcardSubscription, boolean durableSubscription) throws Exception {
            super(brokerService.getBroker(), null, null, new MockConsumerInfo(activemqDestination, networkSubscription, durableSubscription));
            this.wildcardSubscription = wildcardSubscription;
        }

        @Override
        public boolean isWildcard() {
            return this.wildcardSubscription;
        }
    }
}
