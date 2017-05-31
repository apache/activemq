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

import org.apache.activemq.plugin.SubQueueSelectorCacheBroker;

import java.util.Set;

/**
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public class VirtualDestinationSelectorCacheView implements VirtualDestinationSelectorCacheViewMBean {

    private final SubQueueSelectorCacheBroker selectorCacheBroker;

    public VirtualDestinationSelectorCacheView(SubQueueSelectorCacheBroker selectorCacheBroker) {
        this.selectorCacheBroker = selectorCacheBroker;
    }

    @Override
    public Set<String> selectorsForDestination(String destinationName) {
        return selectorCacheBroker.getSelectorsForDestination(destinationName);
    }

    @Override
    public boolean deleteSelectorForDestination(String destinationName, String selector) {
        return selectorCacheBroker.deleteSelectorForDestination(destinationName, selector);
    }

    @Override
    public boolean deleteAllSelectorsForDestination(String destinationName) {
        return selectorCacheBroker.deleteAllSelectorsForDestination(destinationName);
    }
}
