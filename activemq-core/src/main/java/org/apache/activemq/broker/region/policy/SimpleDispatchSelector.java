/**
 * 
 */
package org.apache.activemq.broker.region.policy;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.filter.MessageEvaluationContext;

/**
 * Simple dispatch policy that determines if a message can be sent to a subscription
 *
 * @org.apache.xbean.XBean
 * @version $Revision$
 */
public class SimpleDispatchSelector implements DispatchSelector {

    private final ActiveMQDestination destination;

    /**
     * @param destination
     */
    public SimpleDispatchSelector(ActiveMQDestination destination) {
        this.destination = destination;
    }

    public boolean canDispatch(Subscription subscription, MessageReference node) throws Exception {
        MessageEvaluationContext msgContext = new MessageEvaluationContext();
        msgContext.setDestination(this.destination);
        msgContext.setMessageReference(node);
        return subscription.matches(node, msgContext);
    }
}
