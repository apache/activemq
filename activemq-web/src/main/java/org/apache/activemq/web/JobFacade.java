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
package org.apache.activemq.web;

import javax.management.openmbean.CompositeData;

public class JobFacade {
    private final CompositeData data;
    public JobFacade(CompositeData data) {
        this.data = data;
    }
    public String getCronEntry() {
        return data.get("cronEntry").toString();
    }

    public String getJobId() {
        return toString(data.get("jobId"));
    }

    public String getNextExecutionTime() {
        return toString(data.get("next"));
    }
    
    public long getDelay() {
        Long result = (Long) data.get("delay");
        if (result != null) {
            return result.longValue();
        }
        return 0l;
    }

    public long getPeriod() {
        Long result = (Long) data.get("period");
        if (result != null) {
            return result.longValue();
        }
        return 0l;
    }

    public int getRepeat() {
        Integer result = (Integer) data.get("repeat");
        if (result != null) {
            return result.intValue();
        }
        return 0;
    }

    public String getStart() {
        return toString(data.get("start"));
    }

    private String toString(Object object) {
        return object != null ? object.toString() : "";
    }

}
