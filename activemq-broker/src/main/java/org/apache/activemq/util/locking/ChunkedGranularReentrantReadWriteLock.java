package org.apache.activemq.util.locking;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A chunked GranularReentrantReadWriteLock which has consistent size and doesn't
 * require cleanup or any complex implementation of concurrency complication.
 */
public class ChunkedGranularReentrantReadWriteLock<T> implements GranularReentrantReadWriteLock<T> {

    private int chunks;

    private final ReentrantReadWriteLock[] locks;

    public ChunkedGranularReentrantReadWriteLock() {
        this(1024);
    }

    public ChunkedGranularReentrantReadWriteLock(int chunks) {
        this.chunks = chunks;

        locks = new ReentrantReadWriteLock[chunks];

        for (int i = 0; i < chunks; i++) {
            locks[i] = new ReentrantReadWriteLock();
        }

    }

    @Override
    public ReentrantReadWriteLock.ReadLock readLock(T key) {
        return fetch( key ).readLock();
    }

    @Override
    public ReentrantReadWriteLock.WriteLock writeLock(T key) {
        return fetch( key ).writeLock();
    }

    protected int computeId( T key ) {
        return Math.abs( key.hashCode() ) % chunks;
    }

    protected ReentrantReadWriteLock fetch( T key ) {

        int id = computeId( key );

        return locks[id];

    }

}
