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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import javax.management.j2ee.statistics.Statistic;
import javax.management.j2ee.statistics.Stats;
/**
 * Base class for a Stats implementation
 * 
 * @version $Revision: 1.2 $
 */
public class StatsImpl extends StatisticImpl implements Stats, Resettable {
    //use a Set instead of a Map - to conserve Space
    private Set<StatisticImpl> set;

    public StatsImpl() {
        this(new CopyOnWriteArraySet<StatisticImpl>());
    }

    public StatsImpl(Set<StatisticImpl> set) {
        super("stats", "many", "Used only as container, not Statistic");
        this.set = set;
    }

    public void reset() {
        Statistic[] stats = getStatistics();
        int size = stats.length;
        for (int i = 0; i < size; i++) {
            Statistic stat = stats[i];
            if (stat instanceof Resettable) {
                Resettable r = (Resettable) stat;
                r.reset();
            }
        }
    }

    public Statistic getStatistic(String name) {
        for (StatisticImpl stat : this.set) {
            if (stat.getName() != null && stat.getName().equals(name)) {
                return stat;
            }
        }
        return null;
    }

    public String[] getStatisticNames() {
        List<String> names = new ArrayList<String>();
        for (StatisticImpl stat : this.set) {
            names.add(stat.getName());
        }
        String[] answer = new String[names.size()];
        names.toArray(answer);
        return answer;
    }

    public Statistic[] getStatistics() {
        Statistic[] answer = new Statistic[this.set.size()];
        set.toArray(answer);
        return answer;
    }

    protected void addStatistic(String name, StatisticImpl statistic) {
        this.set.add(statistic);
    }
}
