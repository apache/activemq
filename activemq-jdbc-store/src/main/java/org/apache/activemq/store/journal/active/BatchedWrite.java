/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.store.journal.active;

import org.apache.activemq.store.journal.packet.Packet;

import java.util.concurrent.CountDownLatch;

/**
 * This contains all the data needed to write and force a list of records to a
 * LogFile. The more records that can be cramed into a single BatchedWrite, the
 * higher throughput that can be achived by a write and force operation.
 * 
 * @version $Revision: 1.1 $
 */
final public class BatchedWrite {

    private final Packet packet;
    public Throwable error;
    private Location mark;
    private boolean appendDisabled = false;
    private boolean appendInProgress = false;
    private CountDownLatch writeDoneCountDownLatch;

    /**
     * @param packet
     */
    public BatchedWrite(Packet packet) {
        this.packet = packet;
    }

    /**
     * @throws InterruptedException
     * 
     */
    synchronized private void disableAppend() throws InterruptedException {
        appendDisabled = true;
        while (appendInProgress) {
            wait();
        }
    }

    /**
     * @param packet2
     * @param mark2
     * @return
     */
    public boolean append(Record record, Location recordMark, boolean force) {

        synchronized (this) {
            if (appendDisabled)
                return false;
            appendInProgress = true;
        }
        
        
        if( force && writeDoneCountDownLatch==null)
            writeDoneCountDownLatch = new CountDownLatch(1);
        
        record.read(packet);

        // if we fit the record in this batch
        if ( !record.hasRemaining() ) {
            if (recordMark != null)
                mark = recordMark;
        }

        synchronized (this) {
            appendInProgress = false;
            this.notify();

            if (appendDisabled)
                return false;
            else
                return packet.remaining() > 0;
        }
    }

    public void waitForForce() throws Throwable {
        if( writeDoneCountDownLatch!=null ) {
            writeDoneCountDownLatch.await();
            synchronized (this) {
                if (error != null)
                    throw error;
            }
        }
    }

    public void forced() {
        if( writeDoneCountDownLatch!=null ) {
            writeDoneCountDownLatch.countDown();
        }
    }

    public void writeFailed(Throwable error) {
        if( writeDoneCountDownLatch!=null ) {
            synchronized (this) {
                this.error = error;
            }
            writeDoneCountDownLatch.countDown();
        }
    }

    public Packet getPacket() {
        return packet;
    }

    /**
     * @return
     */
    public Location getMark() {
        return mark;
    }

    /**
     * @throws InterruptedException
     * 
     */
    public void flip() throws InterruptedException {
        disableAppend();
        packet.flip();
    }

    public boolean getForce() {
        return writeDoneCountDownLatch!=null;
    }

}
