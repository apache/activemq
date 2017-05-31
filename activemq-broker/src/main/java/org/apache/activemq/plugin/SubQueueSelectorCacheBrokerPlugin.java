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
package org.apache.activemq.plugin;

import static org.apache.activemq.plugin.SubQueueSelectorCacheBroker.MAX_PERSIST_INTERVAL;

import java.io.File;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;

/**
 * A plugin which allows the caching of the selector from a subscription queue.
 * <p/>
 * This stops the build-up of unwanted messages, especially when consumers may
 * disconnect from time to time when using virtual destinations.
 * <p/>
 * This is influenced by code snippets developed by Maciej Rakowicz
 *
 * @org.apache.xbean.XBean element="virtualSelectorCacheBrokerPlugin"
 */
public class SubQueueSelectorCacheBrokerPlugin implements BrokerPlugin {


    private File persistFile;
    private boolean singleSelectorPerDestination = false;
    private boolean ignoreWildcardSelectors = false;
    private long persistInterval = MAX_PERSIST_INTERVAL;

    @Override
    public Broker installPlugin(Broker broker) throws Exception {
        SubQueueSelectorCacheBroker rc = new SubQueueSelectorCacheBroker(broker, persistFile);
        rc.setSingleSelectorPerDestination(singleSelectorPerDestination);
        rc.setPersistInterval(persistInterval);
        rc.setIgnoreWildcardSelectors(ignoreWildcardSelectors);
        return rc;
    }

    /**
     * Sets the location of the persistent cache
     */
    public void setPersistFile(File persistFile) {
        this.persistFile = persistFile;
    }

    public File getPersistFile() {
        return persistFile;
    }

    public boolean isSingleSelectorPerDestination() {
        return singleSelectorPerDestination;
    }

    public void setSingleSelectorPerDestination(boolean singleSelectorPerDestination) {
        this.singleSelectorPerDestination = singleSelectorPerDestination;
    }

    public long getPersistInterval() {
        return persistInterval;
    }

    public void setPersistInterval(long persistInterval) {
        this.persistInterval = persistInterval;
    }

    public boolean isIgnoreWildcardSelectors() {
        return ignoreWildcardSelectors;
    }

    public void setIgnoreWildcardSelectors(boolean ignoreWildcardSelectors) {
        this.ignoreWildcardSelectors = ignoreWildcardSelectors;
    }
}
