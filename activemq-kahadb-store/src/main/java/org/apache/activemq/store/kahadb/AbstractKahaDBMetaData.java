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

import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;

public abstract class AbstractKahaDBMetaData<T> implements KahaDBMetaData<T> {

    private int state;
    private Location lastUpdateLocation;
    private Page<T> page;

    @Override
    public Page<T> getPage() {
        return page;
    }

    @Override
    public int getState() {
        return state;
    }

    @Override
    public Location getLastUpdateLocation() {
        return lastUpdateLocation;
    }

    @Override
    public void setPage(Page<T> page) {
        this.page = page;
    }

    @Override
    public void setState(int value) {
        this.state = value;
    }

    @Override
    public void setLastUpdateLocation(Location location) {
        this.lastUpdateLocation = location;
    }
}
