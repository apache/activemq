package org.apache.activemq.plugin;

import org.apache.activemq.Service;

import java.util.Set;

public interface SubSelectorCache extends Service {
    Set<String> selectorsForDestination(String destinationName);

    void removeSelectorsForDestination(String destinationName);

    boolean removeSelector(String destinationName, String selector);

    void replaceSelectorsExceptForMatchEverything(String destinationName, String selector);

    void addSelectorForDestination(String destinationName, String selector);
}
