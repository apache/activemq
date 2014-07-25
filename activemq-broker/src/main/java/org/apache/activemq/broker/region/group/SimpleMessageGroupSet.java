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
package org.apache.activemq.broker.region.group;

import java.util.HashSet;
import java.util.Set;

/**
 * A simple implementation which just uses a {@link Set}
 * 
 * 
 */
public class SimpleMessageGroupSet implements MessageGroupSet {

    private Set<String> set = new HashSet<String>();

    public boolean contains(String groupID) {
        return set.contains(groupID);
    }

    public void add(String group) {
        set.add(group);
    }

    protected Set<String> getUnderlyingSet(){
        return set;
    }

}
