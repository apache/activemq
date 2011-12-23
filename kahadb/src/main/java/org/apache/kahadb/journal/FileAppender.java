package org.apache.kahadb.journal;

import java.io.IOException;
import org.apache.kahadb.util.ByteSequence;

public interface FileAppender {
    public static final String PROPERTY_LOG_WRITE_STAT_WINDOW = "org.apache.kahadb.journal.appender.WRITE_STAT_WINDOW";
    public static final int maxStat = Integer.parseInt(System.getProperty(PROPERTY_LOG_WRITE_STAT_WINDOW, "0"));

    Location storeItem(ByteSequence data, byte type, boolean sync) throws IOException;

    Location storeItem(ByteSequence data, byte type, Runnable onComplete) throws IOException;

    void close() throws IOException;
}
