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
package org.apache.activemq.broker.jmx;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.activemq.management.TimeStatisticImpl;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.PersistenceAdapterStatistics;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

public class PersistenceAdapterView implements PersistenceAdapterViewMBean {

    private final static ObjectMapper mapper = new ObjectMapper();
    private final String name;
    private final PersistenceAdapter persistenceAdapter;

    private Callable<String> inflightTransactionViewCallable;
    private Callable<String> dataViewCallable;
    private PersistenceAdapterStatistics persistenceAdapterStatistics;

    public PersistenceAdapterView(PersistenceAdapter adapter) {
        this.name = adapter.toString();
        this.persistenceAdapter = adapter;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTransactions() {
        return invoke(inflightTransactionViewCallable);
    }

    @Override
    public String getData() {
        return invoke(dataViewCallable);
    }

    @Override
    public long getSize() {
        return persistenceAdapter.size();
    }

    @Override
    public String getStatistics() {
        return serializePersistenceAdapterStatistics();
    }

    @Override
    public String resetStatistics() {
        final String result = serializePersistenceAdapterStatistics();

        if (persistenceAdapterStatistics != null) {
            persistenceAdapterStatistics.reset();
        }

        return result;
    }

    private String invoke(Callable<String> callable) {
        String result = null;
        if (callable != null) {
            try {
                result = callable.call();
            } catch (Exception e) {
                result = e.toString();
            }
        }
        return result;
    }

    private String serializePersistenceAdapterStatistics() {
        if (persistenceAdapterStatistics != null) {
            try {
                Map<String, Object> result = new HashMap<String, Object>();
                result.put("slowCleanupTime", getTimeStatisticAsMap(persistenceAdapterStatistics.getSlowCleanupTime()));
                result.put("slowWriteTime", getTimeStatisticAsMap(persistenceAdapterStatistics.getSlowWriteTime()));
                result.put("slowReadTime", getTimeStatisticAsMap(persistenceAdapterStatistics.getSlowReadTime()));
                result.put("writeTime", getTimeStatisticAsMap(persistenceAdapterStatistics.getWriteTime()));
                result.put("readTime", getTimeStatisticAsMap(persistenceAdapterStatistics.getReadTime()));
                return mapper.writeValueAsString(result);
            } catch (IOException e) {
                return e.toString();
            }
        }

        return null;
    }

    private Map<String, Object> getTimeStatisticAsMap(final TimeStatisticImpl timeStatistic) {
        Map<String, Object> result = new HashMap<String, Object>();

        result.put("count", timeStatistic.getCount());
        result.put("maxTime", timeStatistic.getMaxTime());
        result.put("minTime", timeStatistic.getMinTime());
        result.put("totalTime", timeStatistic.getTotalTime());
        result.put("averageTime", timeStatistic.getAverageTime());
        result.put("averageTimeExMinMax", timeStatistic.getAverageTimeExcludingMinMax());
        result.put("averagePerSecond", timeStatistic.getAveragePerSecond());
        result.put("averagePerSecondExMinMax", timeStatistic.getAveragePerSecondExcludingMinMax());

        return result;
    }

    public void setDataViewCallable(Callable<String> dataViewCallable) {
        this.dataViewCallable = dataViewCallable;
    }

    public void setInflightTransactionViewCallable(Callable<String> inflightTransactionViewCallable) {
        this.inflightTransactionViewCallable = inflightTransactionViewCallable;
    }

    public void setPersistenceAdapterStatistics(PersistenceAdapterStatistics persistenceAdapterStatistics) {
        this.persistenceAdapterStatistics = persistenceAdapterStatistics;
    }
}
