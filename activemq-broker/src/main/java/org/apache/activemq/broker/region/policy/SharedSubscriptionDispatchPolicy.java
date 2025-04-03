package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.SharedDurableTopicSubscriptionMetadata;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.filter.MessageEvaluationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class SharedSubscriptionDispatchPolicy {
    public boolean dispatch(MessageReference node, MessageEvaluationContext msgContext, ConcurrentMap<String, SharedDurableTopicSubscriptionMetadata> metadataMap)
            throws Exception {

        // Round-robin dispatching
        System.out.println("[PUB_PATH] In SharedSubscriptionDispatchPolicy");
        int count = 0;

        for (String key : metadataMap.keySet()) {
            System.out.println("[PUB_PATH] SharedSubscriptionDispatchPolicy: subscription name " + key);
            SharedDurableTopicSubscriptionMetadata metadata = metadataMap.get(key);
            Subscription sub = metadata.getNextDurableTopicSubscription();
            if (!sub.matches(node, msgContext)) {
                System.out.println("[PUB_PATH] In SharedSubscriptionDispatchPolicy no match");
                sub.unmatched(node);
                continue;
            }
            sub.add(node);
            count++;
        }

        return count > 0;
    }
}
