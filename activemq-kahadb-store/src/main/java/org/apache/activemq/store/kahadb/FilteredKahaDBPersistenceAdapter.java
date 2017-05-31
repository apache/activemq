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
package org.apache.activemq.store.kahadb;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.StoreUsage;

/**
 * @org.apache.xbean.XBean element="filteredKahaDB"
 *
 */
public class FilteredKahaDBPersistenceAdapter extends DestinationMapEntry {
    private PersistenceAdapter persistenceAdapter;
    private boolean perDestination;
    private StoreUsage usage;

    public FilteredKahaDBPersistenceAdapter() {
        super();
    }

    public FilteredKahaDBPersistenceAdapter(FilteredKahaDBPersistenceAdapter template, ActiveMQDestination destination, PersistenceAdapter adapter) {
        setDestination(destination);
        persistenceAdapter  = adapter;
        if (template.getUsage() != null) {
            usage = template.getUsage().copy();
        }
    }

    public PersistenceAdapter getPersistenceAdapter() {
        return persistenceAdapter;
    }

    public void setPersistenceAdapter(PersistenceAdapter persistenceAdapter) {
        this.persistenceAdapter = persistenceAdapter;
    }

    public boolean isPerDestination() {
        return perDestination;
    }

    public void setPerDestination(boolean perDestination) {
        this.perDestination = perDestination;
    }

    @Override
    public String toString() {
        return "FilteredKahaDBPersistenceAdapter [" + destination + "]";
    }

    @Override
    public int compareTo(Object that) {
        if (that instanceof FilteredKahaDBPersistenceAdapter) {
            return this.destination.compareTo(((FilteredKahaDBPersistenceAdapter) that).destination);
        }
        return super.compareTo(that);
    }

    public void setUsage(StoreUsage usage) {
        this.usage = usage;
    }

    public StoreUsage getUsage() {
        return usage;
    }
}
