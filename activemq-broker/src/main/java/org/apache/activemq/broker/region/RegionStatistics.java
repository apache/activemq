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

import org.apache.activemq.management.CountStatisticImpl;
import org.apache.activemq.management.StatsImpl;

/**
 * The J2EE Statistics for the Connection.
 *
 *
 */
public class RegionStatistics extends StatsImpl {

    private CountStatisticImpl advisoryDestinations;
    private CountStatisticImpl destinations;
    private CountStatisticImpl allDestinations;

    public RegionStatistics() {
        this(true);
    }

    public RegionStatistics(boolean enabled) {

        advisoryDestinations = new CountStatisticImpl("advisoryTopics", "The number of advisory destinations in the region");
        destinations = new CountStatisticImpl("destinations", "The number of regular (non-adivsory) destinations in the region");
        allDestinations = new CountStatisticImpl("allDestinations", "The total number of destinations, including advisory destinations, in the region");

        addStatistic("advisoryDestinations", advisoryDestinations);
        addStatistic("destinations", destinations);
        addStatistic("allDestinations", allDestinations);

        this.setEnabled(enabled);
    }

    public CountStatisticImpl getAdvisoryDestinations() {
        return advisoryDestinations;
    }

    public CountStatisticImpl getDestinations() {
        return destinations;
    }

    public CountStatisticImpl getAllDestinations() {
        return allDestinations;
    }

    public void reset() {
        super.reset();
        advisoryDestinations.reset();
        destinations.reset();
        allDestinations.reset();
    }

    public void setEnabled(boolean enabled) {
        super.setEnabled(enabled);
        advisoryDestinations.setEnabled(enabled);
        destinations.setEnabled(enabled);
        allDestinations.setEnabled(enabled);
    }

    public void setParent(RegionStatistics parent) {
        if (parent != null) {
            advisoryDestinations.setParent(parent.getAdvisoryDestinations());
            destinations.setParent(parent.getDestinations());
            allDestinations.setParent(parent.getAllDestinations());
        } else {
            advisoryDestinations.setParent(null);
            destinations.setParent(null);
            allDestinations.setParent(null);
        }
    }

}
