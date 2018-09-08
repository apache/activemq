package org.apache.activemq.plugin;

import org.apache.activemq.Service;

import java.util.Set;

public interface SubSelectorCache extends Service {
    /**
     *
     * @param destinationName Canonical destination name.
     * @return Mutable copy of selectors. Modifications do not pass through to the cache.
     */
    Set<String> selectorsForDestination(String destinationName);

    void removeSelectorsForDestination(String destinationName);

    boolean removeSelector(String destinationName, String selector);

    /**
     *
     * @param destinationName Canonical destination name.
     * @param selectors Set of selectors to be copied and added to the cache. Future mutations to
     *                  the set will <em>not</em> pass through to the cache.
     */
    void putSelectorsForDestination(String destinationName, Set<String> selectors);
}
