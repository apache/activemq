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
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
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
 * @author Roelof Naude roelof(dot)naude(at)gmail.com
 * @see https://issues.apache.org/activemq/browse/AMQ-3004
 * @see http://mail-archives.apache.org/mod_mbox/activemq-users/201011.mbox/%3C8A013711-2613-450A-A487-379E784AF1D6@homeaway.co.uk%3E
 */
public class SubQueueSelectorCacheBroker extends BrokerFilter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(SubQueueSelectorCacheBroker.class);

    /**
     * The subscription's selector cache. We cache compiled expressions keyed
     * by the target destination.
     */
    private ConcurrentHashMap<String, String> subSelectorCache = new ConcurrentHashMap<String, String>();

    private final File persistFile;

    private boolean running = true;
    private Thread persistThread;
    private static final long MAX_PERSIST_INTERVAL = 600000;
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
    }

    @Override
    public void stop() throws Exception {
        running = false;
        if (persistThread != null) {
            persistThread.interrupt();
            persistThread.join();
        } //if
    }

    @Override
    public Subscription addConsumer(ConnectionContext context, ConsumerInfo info) throws Exception {
        LOG.debug("Caching consumer selector [{}] on a {}", info.getSelector(), info.getDestination().getQualifiedName());
        String selector = info.getSelector();

        // As ConcurrentHashMap doesn't support null values, use always true expression
        if (selector == null) {
            selector = "TRUE";
        }

        subSelectorCache.put(info.getDestination().getQualifiedName(), selector);

        return super.addConsumer(context, info);
    }

    private void readCache() {
        if (persistFile != null && persistFile.exists()) {
            try {
                FileInputStream fis = new FileInputStream(persistFile);
                try {
                    ObjectInputStream in = new ObjectInputStream(fis);
                    try {
                        subSelectorCache = (ConcurrentHashMap<String, String>) in.readObject();
                    } catch (ClassNotFoundException ex) {
                        LOG.error("Invalid selector cache data found. Please remove file.", ex);
                    } finally {
                        in.close();
                    } //try
                } finally {
                    fis.close();
                } //try
            } catch (IOException ex) {
                LOG.error("Unable to read persisted selector cache...it will be ignored!", ex);
            } //try
        } //if
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
                } //try
            } catch (IOException ex) {
                LOG.error("Unable to persist selector cache", ex);
            } finally {
                fos.close();
            } //try
        } catch (IOException ex) {
            LOG.error("Unable to access file[{}]", persistFile, ex);
        } //try
    }

    /**
     * @return The JMS selector for the specified {@code destination}
     */
    public String getSelector(final String destination) {
        return subSelectorCache.get(destination);
    }

    /**
     * Persist the selector cache every {@code MAX_PERSIST_INTERVAL}ms.
     *
     * @see java.lang.Runnable#run()
     */
    public void run() {
        while (running) {
            try {
                Thread.sleep(MAX_PERSIST_INTERVAL);
            } catch (InterruptedException ex) {
            } //try

            persistCache();
        }
    }
}

