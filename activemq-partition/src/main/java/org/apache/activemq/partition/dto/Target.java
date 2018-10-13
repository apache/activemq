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
package org.apache.activemq.partition.dto;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.json.bind.annotation.JsonbProperty;

/**
 * Represents a partition target.  This identifies the brokers that
 * a partition lives on.
 */
public class Target {

    @JsonbProperty("ids")
    public Set<String> ids = new HashSet<String>();

    public Target() {
        ids = new HashSet<String>();
    }

    public Target(String ...ids) {
        this.ids.addAll(java.util.Arrays.asList(ids));
    }

    @Override
    public String toString() {
        return Partitioning.TO_STRING_MAPPER.toJson(this);
    }

    public Set<String> getIds() {
        return ids;
    }

    public void setIds(Collection<String> ids) {
        this.ids = new HashSet<String>(ids);
    }

}
