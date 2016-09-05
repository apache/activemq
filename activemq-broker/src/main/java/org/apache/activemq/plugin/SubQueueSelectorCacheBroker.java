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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.JMException;
import javax.management.ObjectName;

import org.apache.activemq.advisory.AdvisorySupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.VirtualDestinationSelectorCacheView;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A plugin which allows the caching of the selector from a subscription queue.
 * <p/>
 * This stops the build-up of unwanted messages, especially when consumers may
 * disconnect from time to time when using virtual destinations.
 * <p/>
 * This is influenced by code snippets developed by Maciej Rakowicz
 *
 * Refer to:
 * https://issues.apache.org/activemq/browse/AMQ-3004
 * http://mail-archives.apache.org/mod_mbox/activemq-users/201011.mbox/%3C8A013711-2613-450A-A487-379E784AF1D6@homeaway.co.uk%3E
 */
public class SubQueueSelectorCacheBroker extends BrokerFilter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SubQueueSelectorCacheBroker.class);
    public static final String MATCH_EVERYTHING = "TRUE";

    /**
     * The subscription's selector cache. We cache compiled expressions keyed
     * by the target destination.
     */
    private ConcurrentMap<String, Set<String>> subSelectorCache = new ConcurrentHashMap<String, Set<String>>();

    private final File persistFile;
    private boolean singleSelectorPerDestination = false;
    private boolean ignoreWildcardSelectors = false;
    private ObjectName objectName;

    private boolean running = true;
    private final Thread persistThread;
    private long persistInterval = MAX_PERSIST_INTERVAL;
    public static final long MAX_PERSIST_INTERVAL = 600000;
    private static final String SELECTOR_CACHE_PERSIST_THREAD_NAME = "SelectorCachePersistThread";

    /**
     * Constructor
     */
    public SubQueueSelectorCacheBroker(Broker next, final File persistFile) {
        super(next);
        this.persistFile = persistFile;
        LOG.info("Using persisted selector cache from[{}]", persistFile);

        readCache();

        persistThread = new Thread(this, SELECTOR_CACHE_PERSIST_THREAD_NAME);
        persistThread.start();
        enableJmx();
    }

    private void enableJmx() {
        BrokerService broker = getBrokerService();
        if (broker.isUseJmx()) {
            VirtualDestinationSelectorCacheView view = new VirtualDestinationSelectorCacheView(this);
            try {
                objectName = BrokerMBeanSupport.createVirtualDestinationSelectorCacheName(broker.getBrokerObjectName(), "plugin", "virtualDestinationCache");
                LOG.trace("virtualDestinationCacheSelector mbean name; " + objectName.toString());
                AnnotatedMBean.registerMBean(broker.getManagementContext(), view, objectName);
            } catch (Exception e) {
                LOG.warn("JMX is enabled, but when installing the VirtualDestinationSelectorCache, couldn't install the JMX mbeans. Continuing without installing the mbeans.");
            }
        }
    }

    @Override
    public void stop() throws Exception {
        running = false;
        if (persistThread != null) {
            persistThread.interrupt();
            persistThread.join();
        }
        unregisterMBeans();
    }

    private void unregisterMBeans() {
        BrokerService broker = getBrokerService();
        if (broker.isUseJmx() && this.objectName != null) {
            try {
                broker.getManagementContext().unregisterMBean(objectName);
            } catch (JMException e) {
                LOG.warn("Trying uninstall VirtualDestinationSelectorCache; couldn't uninstall mbeans, continuting...");
            }
        }
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        // don't track selectors for advisory topics or temp destinations
        if (!AdvisorySupport.isAdvisoryTopic(info.getDestination()) && !info.getDestination().isTemporary()) {
            String destinationName = info.getDestination().getQualifiedName();
            LOG.debug("Caching consumer selector [{}] on  '{}'", info.getSelector(), destinationName);

            String selector = info.getSelector() == null ? MATCH_EVERYTHING : info.getSelector();

            if (!(ignoreWildcardSelectors && hasWildcards(selector))) {

                Set<String> selectors = subSelectorCache.get(destinationName);
                if (selectors == null) {
                    selectors = Collections.synchronizedSet(new HashSet<String>());
                } else if (singleSelectorPerDestination && !MATCH_EVERYTHING.equals(selector)) {
                    // in this case, we allow only ONE selector. But we don't count the catch-all "null/TRUE" selector
                    // here, we always allow that one. But only one true selector.
                    boolean containsMatchEverything = selectors.contains(MATCH_EVERYTHING);
                    selectors.clear();

                    // put back the MATCH_EVERYTHING selector
                    if (containsMatchEverything) {
                        selectors.add(MATCH_EVERYTHING);
                    }
                }

                LOG.debug("adding new selector: into cache " + selector);
                selectors.add(selector);
                LOG.debug("current selectors in cache: " + selectors);
                subSelectorCache.put(destinationName, selectors);
            }
        }

        return super.addConsumer(context, info);
    }

    static boolean hasWildcards(String selector) {
        return WildcardFinder.hasWildcards(selector);
    }

    @Override
    public void removeConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        if (!AdvisorySupport.isAdvisoryTopic(info.getDestination()) && !info.getDestination().isTemporary()) {
            if (singleSelectorPerDestination) {
                String destinationName = info.getDestination().getQualifiedName();
                Set<String> selectors = subSelectorCache.get(destinationName);
                if (info.getSelector() == null && selectors.size() > 1) {
                    boolean removed = selectors.remove(MATCH_EVERYTHING);
                    LOG.debug("A non-selector consumer has dropped. Removing the catchall matching pattern 'TRUE'. Successful? " + removed);
                }
            }

        }
        super.removeConsumer(context, info);
    }

    @SuppressWarnings("unchecked")
    private void readCache() {
        if (persistFile != null && persistFile.exists()) {
            try {
                try (FileInputStream fis = new FileInputStream(persistFile);) {
                    ObjectInputStream in = new ObjectInputStream(fis);
                    try {
                        subSelectorCache = (ConcurrentHashMap<String, Set<String>>) in.readObject();
                    } catch (ClassNotFoundException ex) {
                        LOG.error("Invalid selector cache data found. Please remove file.", ex);
                    } finally {
                        in.close();
                    }
                }
            } catch (IOException ex) {
                LOG.error("Unable to read persisted selector cache...it will be ignored!", ex);
            }
        }
    }

    /**
     * Persist the selector cache.
     */
    private void persistCache() {
        LOG.debug("Persisting selector cache....");
        try {
            FileOutputStream fos = new FileOutputStream(persistFile);
            try {
                ObjectOutputStream out = new ObjectOutputStream(fos);
                try {
                    out.writeObject(subSelectorCache);
                } finally {
                    out.flush();
                    out.close();
                }
            } catch (IOException ex) {
                LOG.error("Unable to persist selector cache", ex);
            } finally {
                fos.close();
            }
        } catch (IOException ex) {
            LOG.error("Unable to access file[{}]", persistFile, ex);
        }
    }

    /**
     * @return The JMS selector for the specified {@code destination}
     */
    public Set<String> getSelector(final String destination) {
        return subSelectorCache.get(destination);
    }

    /**
     * Persist the selector cache every {@code MAX_PERSIST_INTERVAL}ms.
     *
     * @see java.lang.Runnable#run()
     */
    @Override
    public void run() {
        while (running) {
            try {
                Thread.sleep(persistInterval);
            } catch (InterruptedException ex) {
            }

            persistCache();
        }
    }

    public boolean isSingleSelectorPerDestination() {
        return singleSelectorPerDestination;
    }

    public void setSingleSelectorPerDestination(boolean singleSelectorPerDestination) {
        this.singleSelectorPerDestination = singleSelectorPerDestination;
    }

    @SuppressWarnings("unchecked")
    public Set<String> getSelectorsForDestination(String destinationName) {
        if (subSelectorCache.containsKey(destinationName)) {
            return new HashSet<String>(subSelectorCache.get(destinationName));
        }

        return Collections.EMPTY_SET;
    }

    public long getPersistInterval() {
        return persistInterval;
    }

    public void setPersistInterval(long persistInterval) {
        this.persistInterval = persistInterval;
    }

    public boolean deleteSelectorForDestination(String destinationName, String selector) {
        if (subSelectorCache.containsKey(destinationName)) {
            Set<String> cachedSelectors = subSelectorCache.get(destinationName);
            return cachedSelectors.remove(selector);
        }

        return false;
    }

    public boolean deleteAllSelectorsForDestination(String destinationName) {
        if (subSelectorCache.containsKey(destinationName)) {
            Set<String> cachedSelectors = subSelectorCache.get(destinationName);
            cachedSelectors.clear();
        }
        return true;
    }

    public boolean isIgnoreWildcardSelectors() {
        return ignoreWildcardSelectors;
    }

    public void setIgnoreWildcardSelectors(boolean ignoreWildcardSelectors) {
        this.ignoreWildcardSelectors = ignoreWildcardSelectors;
    }

    // find wildcards inside like operator arguments
    static class WildcardFinder {

        private static final Pattern LIKE_PATTERN=Pattern.compile(
                "\\bLIKE\\s+'(?<like>([^']|'')+)'(\\s+ESCAPE\\s+'(?<escape>.)')?",
                Pattern.CASE_INSENSITIVE);

        private static final String REGEX_SPECIAL = ".+?*(){}[]\\-";

        private static String getLike(final Matcher matcher) {
            return matcher.group("like");
        }

        private static boolean hasLikeOperator(final Matcher matcher) {
            return matcher.find();
        }

        private static String getEscape(final Matcher matcher) {
            String escapeChar = matcher.group("escape");
            if (escapeChar == null) {
                return null;
            } else if (REGEX_SPECIAL.contains(escapeChar)) {
                escapeChar = "\\"+escapeChar;
            }
            return escapeChar;
        }

        private static boolean hasWildcardInCurrentMatch(final Matcher matcher) {
            String wildcards = "[_%]";
            if (getEscape(matcher) != null) {
                wildcards = "(^|[^" + getEscape(matcher) + "])" + wildcards;
            }
            return Pattern.compile(wildcards).matcher(getLike(matcher)).find();
        }

        public static boolean hasWildcards(String selector) {
            Matcher matcher = LIKE_PATTERN.matcher(selector);

            while(hasLikeOperator(matcher)) {
                if (hasWildcardInCurrentMatch(matcher)) {
                    return true;
                }
            }
            return false;
        }
    }
}
