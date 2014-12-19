package org.apache.activemq.util;

import org.apache.activemq.broker.LockableServiceSupport;
import org.apache.activemq.broker.Locker;
import org.apache.activemq.broker.SuppressReplyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @org.apache.xbean.XBean
 */
public class LeaseLockerIOExceptionHandler extends DefaultIOExceptionHandler {
    private static final Logger LOG = LoggerFactory.getLogger(LeaseLockerIOExceptionHandler.class);

    public LeaseLockerIOExceptionHandler() {
        setIgnoreSQLExceptions(false);
        setStopStartConnectors(true);
    }

    // fail only when we get an authoritative answer from the db w/o exceptions
    @Override
    protected boolean hasLockOwnership() throws IOException {
        boolean hasLock = true;

        if (broker.getPersistenceAdapter() instanceof LockableServiceSupport) {
            Locker locker = ((LockableServiceSupport) broker.getPersistenceAdapter()).getLocker();

            if (locker != null) {
                try {
                    if (!locker.keepAlive()) {
                        hasLock = false;
                    }
                }
                catch (SuppressReplyException ignoreWhileHandlingInProgress) {
                }
                catch (IOException ignored) {
                }

                if (!hasLock) {
                    LOG.warn("Lock keepAlive failed, no longer lock owner with: {}", locker);
                    throw new IOException("Lock keepAlive failed, no longer lock owner with: " + locker);
                }
            }
        }

        return hasLock;
    }
}
