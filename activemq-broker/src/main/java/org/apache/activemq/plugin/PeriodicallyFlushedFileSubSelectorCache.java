package org.apache.activemq.plugin;

import static org.apache.activemq.plugin.SubQueueSelectorCacheBroker.MATCH_EVERYTHING;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class PeriodicallyFlushedFileSubSelectorCache implements SubSelectorCache, Runnable {
    public static final long MAX_PERSIST_INTERVAL = 600000;

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicallyFlushedFileSubSelectorCache.class);

    private static final String SELECTOR_CACHE_PERSIST_THREAD_NAME = "SelectorCachePersistThread";

    /**
     * The subscription's selector cache. We cache compiled expressions keyed
     * by the target destination.
     */
    private ConcurrentMap<String, Set<String>> subSelectorCache = new ConcurrentHashMap<String, Set<String>>();

    private long persistInterval = MAX_PERSIST_INTERVAL;

    private boolean running = true;

    private final Thread persistThread;

    private final File persistFile;

    public PeriodicallyFlushedFileSubSelectorCache(File persistFile) {
        this.persistFile = persistFile;
        persistThread = new Thread(this, SELECTOR_CACHE_PERSIST_THREAD_NAME);

        LOG.info("Using persisted selector cache from[{}]", persistFile);

        readCache();
    }

    public long getPersistInterval() {
        return persistInterval;
    }

    public void setPersistInterval(long persistInterval) {
        this.persistInterval = persistInterval;
    }

    @Override
    public Set<String> selectorsForDestination(String destination) {
        Set<String> selectors = subSelectorCache.get(destination);

        if (selectors == null) {
            return Collections.emptySet();
        }

        return ImmutableSet.copyOf(selectors);
    }

    @Override
    public void removeSelectorsForDestination(String destination) {
        if (subSelectorCache.containsKey(destination)) {
            Set<String> cachedSelectors = subSelectorCache.get(destination);
            cachedSelectors.clear();
        }
    }

    @Override
    public boolean removeSelector(String destination, String selector) {
        if (subSelectorCache.containsKey(destination)) {
            Set<String> cachedSelectors = subSelectorCache.get(destination);
            return cachedSelectors.remove(selector);
        }

        return false;
    }

    @Override
    public void replaceSelectorsExceptForMatchEverything(String destinationName, String selector) {
        Set<String> selectors = subSelectorCache.get(destinationName);

        if (selectors != null) {
            boolean containsMatchEverything = selectors.contains(MATCH_EVERYTHING);
            selectors.clear();

            // put back the match everything selector
            if (containsMatchEverything) {
                selectors.add(MATCH_EVERYTHING);
            }
        }

        addSelectorForDestination(destinationName, selector);
    }

    @Override
    public void addSelectorForDestination(String destinationName, String selector) {
        subSelectorCache.putIfAbsent(destinationName, Collections.synchronizedSet(new HashSet<>()));
        subSelectorCache.get(destinationName).add(selector);
    }

    @Override
    public void start() throws Exception {
        persistThread.start();
    }

    @Override
    public void stop() throws InterruptedException {
        running = false;
        if (persistThread != null) {
            persistThread.interrupt();
            persistThread.join();
        }
    }


    @SuppressWarnings("unchecked")
    private void readCache() {
        if (persistFile != null && persistFile.exists()) {
            try {
                try (FileInputStream fis = new FileInputStream(persistFile)) {
                    try (ObjectInputStream in = new ObjectInputStream(fis)) {
                        subSelectorCache = (ConcurrentHashMap<String, Set<String>>) in.readObject();
                    } catch (ClassNotFoundException ex) {
                        LOG.error("Invalid selector cache data found. Please remove file.", ex);
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
}
