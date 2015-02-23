package org.apache.activemq.util.locking;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A simple lock that isn't granular which can be swapped in if there are
 * concerns about any issues with ChunkedGranularReentrantReadWriteLock
 */
public class SimpleGranularReentrantReadWriteLock<T> implements GranularReentrantReadWriteLock<T> {

    private ReentrantReadWriteLock reentrantReadWriteLock;

    public SimpleGranularReentrantReadWriteLock() {
        reentrantReadWriteLock = new ReentrantReadWriteLock();
    }

    @Override
    public ReentrantReadWriteLock.ReadLock readLock(T key) {
        return reentrantReadWriteLock.readLock();
    }

    @Override
    public ReentrantReadWriteLock.WriteLock writeLock(T key) {
        return reentrantReadWriteLock.writeLock();
    }
}
