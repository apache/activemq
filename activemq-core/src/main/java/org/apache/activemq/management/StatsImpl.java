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

import java.util.*;
import javax.management.j2ee.statistics.Statistic;
import javax.management.j2ee.statistics.Stats;


/**
 * Base class for a Stats implementation
 *
 * @version $Revision: 1.2 $
 */
public class StatsImpl extends StatisticImpl implements Stats, Resettable{
    private Map map;

    public StatsImpl() {
        this(new HashMap());
    }

    public StatsImpl(Map map) {
        super("stats", "many", "Used only as container, not Statistic");
        this.map = map;
    }

    public void reset() {
        Statistic[] stats = getStatistics();
        for (int i = 0, size = stats.length; i < size; i++) {
            Statistic stat = stats[i];
            if (stat instanceof Resettable) {
                Resettable r = (Resettable) stat;
                r.reset();
            }
        }
    }

    public Statistic getStatistic(String name) {
        return (Statistic) map.get(name);
    }

    public String[] getStatisticNames() {
        Set keys = map.keySet();
        String[] answer = new String[keys.size()];
        keys.toArray(answer);
        return answer;
    }

    public Statistic[] getStatistics() {
        Collection values = map.values();
        Statistic[] answer = new Statistic[values.size()];
        values.toArray(answer);
        return answer;
    }

    protected void addStatistic(String name, StatisticImpl statistic) {
        map.put(name, statistic);
    }
}
