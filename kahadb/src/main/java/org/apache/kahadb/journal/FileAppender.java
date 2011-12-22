package org.apache.kahadb.journal;

import java.io.IOException;
import org.apache.kahadb.util.ByteSequence;

/**
 * User: gtully
 */
public interface FileAppender {
    Location storeItem(ByteSequence data, byte type, boolean sync) throws IOException;

    Location storeItem(ByteSequence data, byte type, Runnable onComplete) throws IOException;

    void close() throws IOException;
}
