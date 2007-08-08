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
package org.apache.activemq.transport;


import org.apache.activemq.ThreadPriorities;
import org.apache.activemq.util.ServiceStopper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.net.URI;

/**
 * A useful base class for implementations of {@link TransportServer} which uses
 * a background thread to accept new connections.
 * 
 * @version $Revision: 1.1 $
 */
public abstract class TransportServerThreadSupport extends TransportServerSupport implements Runnable {
    private static final Log log = LogFactory.getLog(TransportServerThreadSupport.class);

    private boolean daemon = true;
    private boolean joinOnStop = true;
    private Thread runner;
    private long stackSize=0;//should be a multiple of 128k

    public TransportServerThreadSupport() {
    }

    public TransportServerThreadSupport(URI location) {
        super(location);
    }

    public boolean isDaemon() {
        return daemon;
    }

    /**
     * Sets whether the background read thread is a daemon thread or not
     */
    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    
    public boolean isJoinOnStop() {
        return joinOnStop;
    }

    /**
     * Sets whether the background read thread is joined with (waited for) on a stop
     */
    public void setJoinOnStop(boolean joinOnStop) {
        this.joinOnStop = joinOnStop;
    }

    protected void doStart() throws Exception {
        log.info("Listening for connections at: " + getConnectURI());
        runner = new Thread(null,this, "ActiveMQ Transport Server: "+toString(),stackSize);
        runner.setDaemon(daemon);
        runner.setPriority(ThreadPriorities.BROKER_MANAGEMENT);
        runner.start();
    }

    protected void doStop(ServiceStopper stopper) throws Exception {
        if (runner != null && joinOnStop) {
            runner.join();
            runner = null;
        }
    }

    
    /**
     * @return the stackSize
     */
    public long getStackSize(){
        return this.stackSize;
    }

    
    /**
     * @param stackSize the stackSize to set
     */
    public void setStackSize(long stackSize){
        this.stackSize=stackSize;
    }
}
