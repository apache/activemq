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
package org.apache.activemq.management;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
/**
 * A thread-safe class for a Stats implementation
 */
public class StatsImpl extends StatisticImpl implements Stats, Resettable {
    //use a Set instead of a Map - to conserve Space
    protected final Set<StatisticImpl> set;

    public StatsImpl() {
        this(new CopyOnWriteArraySet<StatisticImpl>());
    }

    public StatsImpl(Set<StatisticImpl> set) {
        super("stats", "many", "Used only as container, not Statistic");
        this.set = set;
    }

    public synchronized void reset() {
        this.set.stream()
            .filter(Resettable.class::isInstance)
            .map(Resettable.class::cast)
            .forEach(resetStat -> resetStat.reset());
    }

    public synchronized Statistic getStatistic(String name) {
        return this.set.stream().filter(s -> s.getName().equals(name)).findFirst().orElse(null);
    }

    public synchronized String[] getStatisticNames() {
        return this.set.stream().map(StatisticImpl::getName).toArray(String[]::new);
    }

    public synchronized Statistic[] getStatistics() {
        return this.set.toArray(new Statistic[this.set.size()]);
    }

    @Deprecated(forRemoval = true, since = "6.2.0")
    protected synchronized void addStatistic(String name, StatisticImpl statistic) {
        this.set.add(statistic);
    }

    protected void addStatistics(Collection<StatisticImpl> statistics) {
        this.set.addAll(statistics);
    }

    protected void removeStatistics(Collection<StatisticImpl> statistics) {
        this.set.removeAll(statistics); 
    }
}
