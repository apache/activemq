package org.apache.activemq.broker.region;

import org.apache.activemq.util.SubscriptionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SharedDurableTopicSubscriptionMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(SharedDurableTopicSubscriptionMetadata.class);

    private String subscriptionName;
    private ArrayList<SubscriptionKey> subKeys = new ArrayList<>();
    private ConcurrentMap<SubscriptionKey, DurableTopicSubscription> subMap = new ConcurrentHashMap<SubscriptionKey, DurableTopicSubscription>();
    private int counter;

    public SharedDurableTopicSubscriptionMetadata(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    public void addDurableTopicSubscription(SubscriptionKey key, DurableTopicSubscription subscription) {
        if (!subMap.containsKey(key)) {
            subKeys.add(key);
            subMap.put(key, subscription);
        }
    }

    public void removeDurableTopicSubscription(SubscriptionKey key, DurableTopicSubscription subscription) {
        if (subMap.containsKey(key)) {
            subMap.remove(key);
            subKeys.remove(key);
        }
    }

    public DurableTopicSubscription getNextDurableTopicSubscription() {
        int index = counter % subKeys.size();
        counter++;
        return subMap.get(subKeys.get(index));
    }
}
