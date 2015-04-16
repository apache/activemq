/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web;

import static com.google.common.collect.Sets.newHashSet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerView;
import org.apache.activemq.broker.jmx.ManagedRegionBroker;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Region;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LocalBrokerFacadeTest {

    @Mock
    private BrokerService brokerService;
    @Mock
    private BrokerView brokerView;
    @Mock
    private Queue queue;
    @Mock
    private Queue otherQueue;
    @Mock
    private ManagedRegionBroker managedRegionBroker;
    @Mock
    private Region region;
    @Mock
    private ActiveMQDestination destination;

    @Test
    public void testPurgeQueueWorksForSimpleQueue() throws Exception {
        LocalBrokerFacade facade = new LocalBrokerFacade(brokerService);
        when(brokerService.getAdminView()).thenReturn(brokerView);
        when(brokerView.getBroker()).thenReturn(managedRegionBroker);
        when(managedRegionBroker.getQueueRegion()).thenReturn(region);
        when(region.getDestinations(destination)).thenReturn(newHashSet((Destination) queue));

        facade.purgeQueue(destination);

        verify(queue).purge();
    }

    @Test
    public void testPurgeQueueWorksForMultipleDestinations() throws Exception {
        Queue queue1 = mock(Queue.class);
        Queue queue2 = mock(Queue.class);

        LocalBrokerFacade facade = new LocalBrokerFacade(brokerService);
        when(brokerService.getAdminView()).thenReturn(brokerView);
        when(brokerView.getBroker()).thenReturn(managedRegionBroker);
        when(managedRegionBroker.getQueueRegion()).thenReturn(region);
        when(region.getDestinations(destination)).thenReturn(newHashSet((Destination) queue1, queue2));

        facade.purgeQueue(destination);

        verify(queue1).purge();
        verify(queue2).purge();
    }

    @Test
    public void testPurgeQueueWorksForFilterWrappedQueue() throws Exception {

        LocalBrokerFacade facade = new LocalBrokerFacade(brokerService);
        when(brokerService.getAdminView()).thenReturn(brokerView);
        when(brokerView.getBroker()).thenReturn(managedRegionBroker);
        when(managedRegionBroker.getQueueRegion()).thenReturn(region);

        when(region.getDestinations(destination)).thenReturn(newHashSet((Destination) new DestinationFilter(queue)));

        facade.purgeQueue(destination);

        verify(queue).purge();
    }

    @Test
    public void testPurgeQueueWorksForMultipleFiltersWrappingAQueue() throws Exception {

        LocalBrokerFacade facade = new LocalBrokerFacade(brokerService);
        when(brokerService.getAdminView()).thenReturn(brokerView);
        when(brokerView.getBroker()).thenReturn(managedRegionBroker);
        when(managedRegionBroker.getQueueRegion()).thenReturn(region);

        when(region.getDestinations(destination)).thenReturn(newHashSet((Destination) new DestinationFilter(new DestinationFilter(queue))));

        facade.purgeQueue(destination);

        verify(queue).purge();
    }
}
