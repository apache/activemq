/**
 *
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
package org.apache.activemq.web.controller;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.web.DestinationFacade;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.Controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Set;
import java.util.Iterator;

/**
 *
 * @version $Revision$
 */
public class PurgeDestination extends DestinationFacade implements Controller {

    public PurgeDestination(BrokerService brokerService) {
        super(brokerService);
    }

    public ModelAndView handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception {
        purgeDestination();
        return redirectToBrowseView();
    }

    public void purgeDestination() throws Exception {
        if (isQueue()) {
            Set destinations = getManagedBroker().getQueueRegion().getDestinations(createDestination());
            for (Iterator i=destinations.iterator(); i.hasNext();) {
                Queue regionQueue = (Queue)i.next();
                regionQueue.purge();
            }
        }
        else {
            throw new UnsupportedOperationException("Purge supported for queues only. Receieved JMSDestinationType=" + getJMSDestinationType());
        }
    }
}

