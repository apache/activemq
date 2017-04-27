package org.apache.activemq.util.locking;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A granular locking system which allocates one lock per key to improve
 * concurrency.
 */
public interface GranularReentrantReadWriteLock<T> {

    public ReentrantReadWriteLock.ReadLock readLock(T key);

    public ReentrantReadWriteLock.WriteLock writeLock(T key);

}
