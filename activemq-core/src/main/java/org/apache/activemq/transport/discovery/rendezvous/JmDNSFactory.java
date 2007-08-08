/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.discovery.rendezvous;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import javax.jmdns.JmDNS;
import java.util.concurrent.atomic.AtomicInteger;

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
