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

import java.util.concurrent.Callable;
import org.apache.activemq.store.PersistenceAdapter;

public class PersistenceAdapterView implements PersistenceAdapterViewMBean {

    private final String name;
    private final PersistenceAdapter persistenceAdapter;

    private Callable<String> inflightTransactionViewCallable;
    private Callable<String> dataViewCallable;

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

    public void setDataViewCallable(Callable<String> dataViewCallable) {
        this.dataViewCallable = dataViewCallable;
    }

    public void setInflightTransactionViewCallable(Callable<String> inflightTransactionViewCallable) {
        this.inflightTransactionViewCallable = inflightTransactionViewCallable;
    }
}
