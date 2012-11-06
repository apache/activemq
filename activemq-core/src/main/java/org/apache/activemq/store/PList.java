package org.apache.activemq.store;

import org.apache.activemq.util.ByteSequence;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface PList {
    void setName(String name);

    String getName();

    void destroy() throws IOException;

    void addLast(String id, ByteSequence bs) throws IOException;

    void addFirst(String id, ByteSequence bs) throws IOException;

    boolean remove(String id) throws IOException;

    boolean remove(long position) throws IOException;

    PListEntry get(long position) throws IOException;

    PListEntry getFirst() throws IOException;

    PListEntry getLast() throws IOException;

    boolean isEmpty();

    PListIterator iterator() throws IOException;

    long size();

    public interface PListIterator extends Iterator<PListEntry> {
        void release();
    }
}
