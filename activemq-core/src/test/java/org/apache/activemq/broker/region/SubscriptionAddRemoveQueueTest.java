package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import javax.jms.InvalidSelectorException;
import javax.management.ObjectName;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatchNotification;
import org.apache.activemq.command.MessagePull;
import org.apache.activemq.command.Response;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.thread.TaskRunnerFactory;

public class SubscriptionAddRemoveQueueTest extends TestCase {

    Queue queue;
    Message msg = new ActiveMQMessage();
    ConsumerInfo info = new ConsumerInfo();
    List<SimpleImmediateDispatchSubscription> subs = new ArrayList<SimpleImmediateDispatchSubscription>();
    ConnectionContext context = new ConnectionContext();
    int numSubscriptions = 1000;
    boolean working = true;
    int senders = 20;
    
    
    @Override
    public void setUp() throws Exception {
        BrokerService brokerService = new BrokerService();
        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        DestinationStatistics parentStats = new DestinationStatistics();
        parentStats.setEnabled(true);
        
        TaskRunnerFactory taskFactory = null;
        MessageStore store = null;
        
        msg.setDestination(destination);
        info.setDestination(destination);
        info.setPrefetchSize(100);
        
        queue = new Queue(brokerService, destination, store, parentStats, taskFactory);
        queue.initialize();
    }
    
    public void testNoDispatchToRemovedConsumers() throws Exception {
        Runnable sender = new Runnable() {
            public void run() {
                while (working) {
                    try {
                        queue.sendMessage(context, msg);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception in sendMessage, ex:" + e);
                    }
                }
            }
        };
        
        Runnable subRemover = new Runnable() {
            public void run() {
                for (Subscription sub : subs) {
                    try {
                        queue.removeSubscription(context, sub);
                    } catch (Exception e) {
                        e.printStackTrace();
                        fail("unexpected exception in removeSubscription, ex:" + e);
                    }
                }
            }  
        };

        for (int i=0;i<numSubscriptions; i++) {
            SimpleImmediateDispatchSubscription sub = new SimpleImmediateDispatchSubscription();
            subs.add(sub);
            queue.addSubscription(context, sub);
        }
        assertEquals("there are X subscriptions", numSubscriptions, queue.getDestinationStatistics().getConsumers().getCount());
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i=0; i<senders ; i++) {
            executor.submit(sender);
        }
        
        Thread.sleep(1000);
        for (SimpleImmediateDispatchSubscription sub : subs) {
            assertTrue("There are some locked messages in the subscription", hasSomeLocks(sub.dispatched));
        }
        
        Future<?> result = executor.submit(subRemover);
        result.get();
        working = false;
        assertEquals("there are no subscriptions", 0, queue.getDestinationStatistics().getConsumers().getCount());
        
        for (SimpleImmediateDispatchSubscription sub : subs) {
            assertTrue("There are no locked messages in any removed subscriptions", !hasSomeLocks(sub.dispatched));
        }
        
    }
    
    private boolean hasSomeLocks(List<MessageReference> dispatched) {
        boolean hasLock = false;
        for (MessageReference mr: dispatched) {
            QueueMessageReference qmr = (QueueMessageReference) mr;
            if (qmr.getLockOwner() != null) {
                hasLock = true;
                break;
            }
        }
        return hasLock;
    }

    public class SimpleImmediateDispatchSubscription implements Subscription, LockOwner {

        List<MessageReference> dispatched = 
            Collections.synchronizedList(new ArrayList<MessageReference>());

        public void acknowledge(ConnectionContext context, MessageAck ack)
                throws Exception {
            // TODO Auto-generated method stub

        }

        public void add(MessageReference node) throws Exception {
            // immediate dispatch
            QueueMessageReference  qmr = (QueueMessageReference)node;
            qmr.lock(this);
            dispatched.add(qmr);
        }

        public void add(ConnectionContext context, Destination destination)
                throws Exception {
            // TODO Auto-generated method stub

        }

        public void destroy() {
            // TODO Auto-generated method stub

        }

        public void gc() {
            // TODO Auto-generated method stub

        }

        public ConsumerInfo getConsumerInfo() {
            return info;
        }

        public long getDequeueCounter() {
            // TODO Auto-generated method stub
            return 0;
        }

        public long getDispatchedCounter() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int getDispatchedQueueSize() {
            // TODO Auto-generated method stub
            return 0;
        }

        public long getEnqueueCounter() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int getInFlightSize() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int getInFlightUsage() {
            // TODO Auto-generated method stub
            return 0;
        }

        public ObjectName getObjectName() {
            // TODO Auto-generated method stub
            return null;
        }

        public int getPendingQueueSize() {
            // TODO Auto-generated method stub
            return 0;
        }

        public int getPrefetchSize() {
            // TODO Auto-generated method stub
            return 0;
        }

        public String getSelector() {
            // TODO Auto-generated method stub
            return null;
        }

        public boolean isBrowser() {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isFull() {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isHighWaterMark() {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isLowWaterMark() {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isRecoveryRequired() {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean isSlave() {
            // TODO Auto-generated method stub
            return false;
        }

        public boolean matches(MessageReference node,
                MessageEvaluationContext context) throws IOException {
            return true;
        }

        public boolean matches(ActiveMQDestination destination) {
            // TODO Auto-generated method stub
            return false;
        }

        public void processMessageDispatchNotification(
                MessageDispatchNotification mdn) throws Exception {
            // TODO Auto-generated method stub

        }

        public Response pullMessage(ConnectionContext context, MessagePull pull)
                throws Exception {
            // TODO Auto-generated method stub
            return null;
        }

        public List<MessageReference> remove(ConnectionContext context,
                Destination destination) throws Exception {
            return new ArrayList<MessageReference>(dispatched);
        }

        public void setObjectName(ObjectName objectName) {
            // TODO Auto-generated method stub

        }

        public void setSelector(String selector)
                throws InvalidSelectorException, UnsupportedOperationException {
            // TODO Auto-generated method stub

        }

        public void updateConsumerPrefetch(int newPrefetch) {
            // TODO Auto-generated method stub

        }

        public boolean addRecoveredMessage(ConnectionContext context,
                MessageReference message) throws Exception {
            // TODO Auto-generated method stub
            return false;
        }

        public ActiveMQDestination getActiveMQDestination() {
            // TODO Auto-generated method stub
            return null;
        }

        public int getLockPriority() {
            // TODO Auto-generated method stub
            return 0;
        }

        public boolean isLockExclusive() {
            // TODO Auto-generated method stub
            return false;
        }

        public void addDestination(Destination destination) {            
        }

        public void removeDestination(Destination destination) {            
        }

    }
}
