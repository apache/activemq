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
package org.apache.activemq.store.journal.packet;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;


/**
 * Provides a Packet implementation that filters operations to another packet.
 * 
 * Used to make it easier to augment the {@see #narrow(Class)}method.
 * 
 * @version $Revision$
 */
public abstract class FilterPacket implements Packet {
    final protected Packet next;

    public FilterPacket(Packet next) {
        this.next = next;
    }

    public ByteSequence asByteSequence() {
        return next.asByteSequence();
    }

    public int capacity() {
        return next.capacity();
    }

    public void clear() {
        next.clear();
    }

    public void flip() {
        next.flip();
    }

    public boolean hasRemaining() {
        return next.hasRemaining();
    }

    public int limit() {
        return next.limit();
    }

    public void limit(int limit) {
        next.limit(limit);
    }

    public Object getAdapter(Class target) {
        if( target.isAssignableFrom(getClass()) ) {
            return this;
        }
        return next.getAdapter(target);
    }

    public int position() {
        return next.position();
    }

    public void position(int position) {
        next.position(position);
    }

    public int read() {
        return next.read();
    }

    public int read(byte[] data, int offset, int length) {
        return next.read(data, offset, length);
    }

    public int read(Packet dest) {
        return next.read(dest);
    }

    public int remaining() {
        return next.remaining();
    }

    public void rewind() {
        next.rewind();
    }

    public byte[] sliceAsBytes() {
        return next.sliceAsBytes();
    }

    public int write(byte[] data, int offset, int length) {
        return next.write(data, offset, length);
    }

    public boolean write(int data) {
        return next.write(data);
    }

    public void writeTo(OutputStream out) throws IOException {
        next.writeTo(out);
    }
    public void writeTo(DataOutput out) throws IOException {
        next.writeTo(out);
    }

    public Object duplicate(ClassLoader cl) throws IOException {
        return next.duplicate(cl);
    }

    public Packet duplicate() {
        return filter(next.duplicate());
    }

    public Packet slice() {
        return filter(next.slice());
    }
    
    public void dispose() {
        next.dispose();
    }

    abstract public Packet filter(Packet packet);
}
