package org.apache.activemq.broker.region.policy;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.Connection;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.thread.Scheduler;
import org.apache.activemq.transport.InactivityIOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Abort slow consumers when they reach the configured threshold of slowness, default is slow for 30 seconds
 * 
 * @org.apache.xbean.XBean
 */
public class AbortSlowConsumerStrategy implements SlowConsumerStrategy, Runnable {
    
    private static final Log LOG = LogFactory.getLog(AbortSlowConsumerStrategy.class);

    private static final Scheduler scheduler = Scheduler.getInstance();
    private AtomicBoolean taskStarted = new AtomicBoolean(false);
    private Map<Subscription, SlowConsumerEntry> slowConsumers = new ConcurrentHashMap<Subscription, SlowConsumerEntry>();

    private long maxSlowCount = -1;
    private long maxSlowDuration = 30*1000;
    private long checkPeriod = 30*1000;
    private boolean abortConnection = false;

    public void slowConsumer(ConnectionContext context, Subscription subs) {
        if (maxSlowCount < 0 && maxSlowDuration < 0) {
            // nothing to do
            LOG.info("no limits set, slowConsumer strategy has nothing to do");
            return;
        }
        
        if (taskStarted.compareAndSet(false, true)) {
            scheduler.executePeriodically(this, checkPeriod);
        }
            
        if (!slowConsumers.containsKey(subs)) {
            slowConsumers.put(subs, new SlowConsumerEntry(context));
        } else if (maxSlowCount > 0) {
            slowConsumers.get(subs).slow();
        }
    }

    public void run() {
        if (maxSlowDuration > 0) {
            // mark
            for (SlowConsumerEntry entry : slowConsumers.values()) {
                entry.mark();
            }
        }
        
        HashMap<Subscription, SlowConsumerEntry> toAbort = new HashMap<Subscription, SlowConsumerEntry>();
        for (Entry<Subscription, SlowConsumerEntry> entry : slowConsumers.entrySet()) {
            if (entry.getKey().isSlowConsumer()) {
                if (maxSlowDuration > 0 && (entry.getValue().markCount * checkPeriod > maxSlowDuration)
                        || maxSlowCount > 0 && entry.getValue().slowCount > maxSlowCount) { 
                    toAbort.put(entry.getKey(), entry.getValue());
                    slowConsumers.remove(entry.getKey());
                }
            } else {
                LOG.info("sub: " + entry.getKey().getConsumerInfo().getConsumerId() + " is no longer slow");
                slowConsumers.remove(entry.getKey());
            }
        }
        
        for (final Entry<Subscription, SlowConsumerEntry> entry : toAbort.entrySet()) {
            ConnectionContext connectionContext = entry.getValue().context;
            if (connectionContext!= null) {
                try {
                    LOG.info("aborting " 
                            + (abortConnection ? "connection" : "consumer") 
                            + ", slow consumer: " + entry.getKey().getConsumerInfo().getConsumerId()); 

                    final Connection connection = connectionContext.getConnection();
                    if (connection != null) {    
                        if (abortConnection) {
                            scheduler.executeAfterDelay(new Runnable() {
                                public void run() {
                                    connection.serviceException(new InactivityIOException("Consumer was slow too often (>" 
                                            + maxSlowCount +  ") or too long (>"
                                            + maxSlowDuration + "): " + entry.getKey().getConsumerInfo().getConsumerId()));
                                }}, 0l);
                        } else {
                            // just abort the consumer by telling it to stop
                            ConsumerControl stopConsumer = new ConsumerControl();
                            stopConsumer.setConsumerId(entry.getKey().getConsumerInfo().getConsumerId());
                            stopConsumer.setClose(true);    
                            connection.dispatchAsync(stopConsumer);
                        }
                    } else {
                        LOG.debug("slowConsumer abort ignored, no connection in context:"  + connectionContext);
                    }
                } catch (Exception e) {
                    LOG.info("exception on stopping " 
                            + (abortConnection ? "connection" : "consumer") 
                            + " to abort slow consumer: " + entry.getKey(), e);   
                }
            }
        }
    }
    
    public long getMaxSlowCount() {
        return maxSlowCount;
    }

    /**
     * number of times a subscription can be deemed slow before triggering abort
     * effect depends on dispatch rate as slow determination is done on dispatch
     */
    public void setMaxSlowCount(int maxSlowCount) {
        this.maxSlowCount = maxSlowCount;
    }

    public long getMaxSlowDuration() {
        return maxSlowDuration;
    }

    /**
     * time in milliseconds that a sub can remain slow before triggering
     * an abort.
     * @param maxSlowDuration
     */
    public void setMaxSlowDuration(long maxSlowDuration) {
        this.maxSlowDuration = maxSlowDuration;
    }

    public long getCheckPeriod() {
        return checkPeriod;
    }

    /**
     * time in milliseconds between checks for slow subscriptions
     * @param checkPeriod
     */
    public void setCheckPeriod(long checkPeriod) {
        this.checkPeriod = checkPeriod;
    }

    public boolean isAbortConnection() {
        return abortConnection;
    }

    /**
     * abort the consumers connection rather than sending a stop command to the remote consumer
     * @param abortConnection
     */
    public void setAbortConnection(boolean abortConnection) {
        this.abortConnection = abortConnection;
    }

    static class SlowConsumerEntry {
        
        final ConnectionContext context;
        int slowCount = 1;
        int markCount = 0;
        
        SlowConsumerEntry(ConnectionContext context) {
            this.context = context;
        }

        public void slow() {
            slowCount++;
        }
        
        public void mark() {
            markCount++;
        }
    }
}
