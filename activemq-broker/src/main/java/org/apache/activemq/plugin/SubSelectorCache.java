package org.apache.activemq.plugin;

import org.apache.activemq.Service;

import java.util.Set;

/**
 * Cache for set of JMS selectors used by qualified destination name.
 *
 * @see SubQueueSelectorCacheBroker
 */
public interface SubSelectorCache extends Service {
    /**
     * @param destinationName Qualified destination name
     * @return Immutable copy of current known selectors for given destination
     */
    Set<String> selectorsForDestination(String destinationName);

    /**
     * Removes all selectors from cache for destination.
     * @param destinationName Qualified destination name
     */
    void removeSelectorsForDestination(String destinationName);

    /**
     * Removes single selector if present from cache.
     * @param destinationName Qualified destination name
     * @param selector Selector expression to remove
     * @return {@code true} if selector found and removed, {@code false} otherwise
     */
    boolean removeSelector(String destinationName, String selector);

    /**
     * Replaces all selectors for destination, except for the
     * {@link SubQueueSelectorCacheBroker#MATCH_EVERYTHING} selector, with provided
     * {@code selector}.
     * @param destinationName Qualified destination name
     * @param selector Selector expression
     */
    void replaceSelectorsExceptForMatchEverything(String destinationName, String selector);

    /**
     * Add {@code selector} to set of cached selectors for given destination.
     *
     * <p>Selector may not immediately be persisted in the cache.
     *
     * @param destinationName Qualified destination name
     * @param selector Selector expression to cache
     */
    void addSelectorForDestination(String destinationName, String selector);
}
