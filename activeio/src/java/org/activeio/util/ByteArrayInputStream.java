package org.activeio.util;

import java.io.IOException;
import java.io.InputStream;

import org.activeio.packet.ByteSequence;

/**
 * Very similar to the java.io.ByteArrayInputStream but this version 
 * is not thread safe.
 */
public class ByteArrayInputStream extends InputStream {

    byte buffer[];
    int limit;
    int pos;
    int mark;

    public ByteArrayInputStream(byte data[]) {
        this(data, 0, data.length);
    }
    
    public ByteArrayInputStream(ByteSequence sequence) {
        this(sequence.getData(), sequence.getOffset(), sequence.getLength());
    }

    public ByteArrayInputStream(byte data[], int offset, int size) {
        this.buffer = data;
        this.mark= this.pos = offset;        
        this.limit = offset+size;
    }
    
    public int read() throws IOException {
        if( pos < limit )
            return buffer[pos++] & 0xff;
        else
            return -1;
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }
    
    public int read(byte b[], int off, int len) {
        if (pos < limit) {
            len = Math.min(len, limit-pos);
            if (len > 0) {
                System.arraycopy(buffer, pos, b, off, len);
                pos += len;
            }
            return len;
        } else {
            return -1;
        }
    }

    public long skip(long len) throws IOException {
        if (pos < limit) {
            len = Math.min(len, limit-pos);
            if (len > 0) {
                pos += len;
            }
            return len;
        } else {
            return -1;
        }
    }
    
    public int available() {
        return limit - pos;
    }

    public boolean markSupported() {
        return true;
    }
    
    public void mark(int markpos) {
        mark = pos;
    }
    
    public void reset() {
        pos = mark;
    }
}
