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

import java.util.Set;

/**
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public interface VirtualDestinationSelectorCacheViewMBean {

    @MBeanInfo("Dump raw cache of selectors organized by destination")
    public Set<String> selectorsForDestination(String destinationName);

    @MBeanInfo("Delete a selector for a destination. Selector must match what returns from selectorsForDestination operation")
    public boolean deleteSelectorForDestination(String destinationName, String selector);

    @MBeanInfo("Dump raw cache of selectors organized by destination")
    public boolean deleteAllSelectorsForDestination(String destinationName);

}
