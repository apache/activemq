package org.apache.activemq.util.locking;

import junit.framework.TestCase;

public class ChunkedGranularReentrantReadWriteLockTest extends TestCase {

    public void testComputeId() throws Exception {

        ChunkedGranularReentrantReadWriteLock<Integer> granularReentrantReadWriteLock =
          new ChunkedGranularReentrantReadWriteLock<>();

        assertEquals( 0, granularReentrantReadWriteLock.computeId( 0 ) );
        assertEquals( 500, granularReentrantReadWriteLock.computeId( 500 ) );
        assertEquals( 1023, granularReentrantReadWriteLock.computeId( 1023 ) );

        // test rollover..
        assertEquals( 0, granularReentrantReadWriteLock.computeId( 1024 ) );
        assertEquals( 1, granularReentrantReadWriteLock.computeId( 1025 ) );

    }

    public void testLockAcquireAndRelease() throws Exception {

        ChunkedGranularReentrantReadWriteLock<Integer> granularReentrantReadWriteLock =
          new ChunkedGranularReentrantReadWriteLock<>();

        granularReentrantReadWriteLock.readLock( 0 ).lock();

        assertEquals( 1, granularReentrantReadWriteLock.fetch( 0 ).getReadHoldCount() );

        granularReentrantReadWriteLock.readLock( 0 ).unlock();

        assertEquals( 0, granularReentrantReadWriteLock.fetch( 0 ).getReadHoldCount() );

    }

}