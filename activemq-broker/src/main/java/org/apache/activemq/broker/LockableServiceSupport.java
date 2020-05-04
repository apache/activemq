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
package org.apache.activemq.broker;

import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.util.ServiceSupport;
import org.apache.activemq.util.ThreadPoolUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Helper class for working with services that requires locking
 */
public abstract class LockableServiceSupport extends ServiceSupport implements Lockable, BrokerServiceAware {

    private static final Logger LOG = LoggerFactory.getLogger(LockableServiceSupport.class);
    boolean useLock = true;
    boolean stopOnError = false;
    Locker locker;
    long lockKeepAlivePeriod = 0;
    private ScheduledFuture<?> keepAliveTicket;
    protected ScheduledThreadPoolExecutor clockDaemon;
    protected BrokerService brokerService;

    /**
     * Initialize resources before locking
     *
     * @throws Exception
     */
    abstract public void init() throws Exception;

    @Override
    public void setUseLock(boolean useLock) {
        this.useLock = useLock;
    }

    public boolean isUseLock() {
        return this.useLock;
    }

    @Override
    public void setStopOnError(boolean stopOnError) {
        this.stopOnError = stopOnError;
    }

    public boolean isStopOnError() {
        return this.stopOnError;
    }

    @Override
    public void setLocker(Locker locker) throws IOException {
        this.locker = locker;
        locker.setLockable(this);
        if (this instanceof PersistenceAdapter) {
            this.locker.configure((PersistenceAdapter)this);
        }
    }

    public Locker getLocker() throws IOException {
        if (this.locker == null) {
            setLocker(createDefaultLocker());
        }
        return this.locker;
    }

    @Override
    public void setLockKeepAlivePeriod(long lockKeepAlivePeriod) {
        this.lockKeepAlivePeriod = lockKeepAlivePeriod;
    }

    @Override
    public long getLockKeepAlivePeriod() {
        return lockKeepAlivePeriod;
    }

    @Override
    public void preStart() throws Exception {
        init();
        if (useLock) {
            if (getLocker() == null) {
                LOG.warn("No locker configured");
            } else {
                getLocker().start();
                if (lockKeepAlivePeriod > 0) {
                    keepAliveTicket = getScheduledThreadPoolExecutor().scheduleAtFixedRate(new Runnable() {
                        public void run() {
                            keepLockAlive();
                        }
                    }, lockKeepAlivePeriod, lockKeepAlivePeriod, TimeUnit.MILLISECONDS);
                }
            }
        }
    }

    @Override
    public void postStop(ServiceStopper stopper) throws Exception {
        if (useLock) {
            if (keepAliveTicket != null) {
                keepAliveTicket.cancel(false);
                keepAliveTicket = null;
            }
            if (locker != null) {
                getLocker().stop();
                locker = null;
            }
        }
        ThreadPoolUtils.shutdown(clockDaemon);
        clockDaemon = null;
    }

    protected void keepLockAlive() {
        boolean stop = false;
        try {
            Locker locker = getLocker();
            if (locker != null) {
                if (!locker.keepAlive()) {
                    stop = true;
                }
            }
        } catch (SuppressReplyException e) {
            if (stopOnError) {
                stop = true;
            }
            LOG.warn("locker keepAlive resulted in", e);
        } catch (IOException e) {
            if (stopOnError) {
                stop = true;
            }
            LOG.warn("locker keepAlive resulted in", e);
        }
        if (stop) {
            stopBroker();
        }
    }

    protected void stopBroker() {
        // we can no longer keep the lock so lets fail
        LOG.error("{}, no longer able to keep the exclusive lock so giving up being a master", brokerService.getBrokerName());
        try {
            if( brokerService.isRestartAllowed() ) {
                brokerService.requestRestart();
            }
            brokerService.stop();
        } catch (Exception e) {
            LOG.warn("Failure occurred while stopping broker");
        }
    }

    public ScheduledThreadPoolExecutor getScheduledThreadPoolExecutor() {
        if (clockDaemon == null) {
            clockDaemon = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
                public Thread newThread(Runnable runnable) {
                    Thread thread = new Thread(runnable, "ActiveMQ Lock KeepAlive Timer");
                    thread.setDaemon(true);
                    return thread;
                }
            });
        }
        return clockDaemon;
    }

    public void setScheduledThreadPoolExecutor(ScheduledThreadPoolExecutor clockDaemon) {
        this.clockDaemon = clockDaemon;
    }

    @Override
    public void setBrokerService(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public BrokerService getBrokerService() {
        return this.brokerService;
    }
}
