package org.activemq.transport.discovery.rendezvous;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import javax.jmdns.JmDNS;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

public class JmDNSFactory {
    
    static Map registry = new HashMap();
    static class UsageTracker {
        AtomicInteger count = new AtomicInteger(0);
        JmDNS jmDNS; 
    }
    
    static synchronized JmDNS create(final InetAddress address) throws IOException {
        UsageTracker tracker = (UsageTracker)registry.get(address);
        if( tracker == null ) {
            tracker = new UsageTracker();
            tracker.jmDNS = new JmDNS(address) {
                public void close() {
                    if( onClose(address) ) {
                        super.close();
                    }
                }
            };
            registry.put(address, tracker);
        } 
        tracker.count.incrementAndGet();
        return tracker.jmDNS;
    }
    
    static synchronized boolean onClose(InetAddress address){
        UsageTracker tracker=(UsageTracker) registry.get(address);
        if(tracker!=null){
            if(tracker.count.decrementAndGet()==0){
                registry.remove(address);
                return true;
            }
        }
        return false;
    }
}
