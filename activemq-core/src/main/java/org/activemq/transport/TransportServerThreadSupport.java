/**
 * 
 * Copyright 2005 LogicBlaze, Inc. http://www.logicblaze.com
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 **/
package org.activemq.transport;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

import org.activemq.ThreadPriorities;
import org.activemq.util.ServiceStopper;
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

    private AtomicBoolean closed = new AtomicBoolean(false);
    private AtomicBoolean started = new AtomicBoolean(false);
    private AtomicBoolean closing = new AtomicBoolean(false);
    private boolean daemon = true;
    private Thread runner;

    public TransportServerThreadSupport() {
    }

    public TransportServerThreadSupport(URI location) {
        super(location);
    }

    public void start() throws Exception {
        if (started.compareAndSet(false, true)) {
            log.info("Listening for connections at: " + getLocation());
            runner = new Thread(this, toString());
            runner.setDaemon(daemon);
            runner.setPriority(ThreadPriorities.BROKER_MANAGEMENT);
            runner.start();
        }
    }

    public void stop() throws Exception {
        if (closed.compareAndSet(false, true)) {
            closing.set(true);
            ServiceStopper stopper = new ServiceStopper();
            try {
                doStop(stopper);
            }
            catch (Exception e) {
                stopper.onException(this, e);
            }
            if (runner != null) {
                runner.join();
                runner = null;
            }
            closed.set(true);
            started.set(false);
            closing.set(false);
            stopper.throwFirstException();
        }
    }

    public boolean isStarted() {
        return started.get();
    }
    
    /**
     * @return true if the transport server is in the process of closing down.
     */
    public boolean isClosing() {
        return closing.get();
    }

    public boolean isClosed() {
        return closed.get();
    }

    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    protected abstract void doStop(ServiceStopper stopper) throws Exception;
}
