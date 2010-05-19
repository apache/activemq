package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;

/*
 * a strategy for dealing with slow consumers
 */
public interface SlowConsumerStrategy {

    void slowConsumer(ConnectionContext context, Subscription subs);

}
