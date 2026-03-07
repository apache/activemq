// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.
// http://code.google.com/p/protobuf/
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.activemq.protobuf;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reads and decodes protocol message fields.
 * 
 * This class contains two kinds of methods: methods that read specific protocol
 * message constructs and field types (e.g. {@link #readTag()} and
 * {@link #readInt32()}) and methods that read low-level values (e.g.
 * {@link #readRawVarint32()} and {@link #readRawBytes}). If you are reading
 * encoded protocol messages, you should use the former methods, but if you are
 * reading some other format of your own design, use the latter.
 * 
 * @author kenton@google.com Kenton Varda
 */
public final class CodedInputStream extends FilterInputStream {

    private int lastTag = 0;
    private int limit = Integer.MAX_VALUE;
    private int pos;
    private BufferInputStream bis;
    
    public CodedInputStream(InputStream in) {
        super(in);
        if( in.getClass() == BufferInputStream.class ) {
            bis = (BufferInputStream)in;
        }
    }

    public CodedInputStream(Buffer data) {
        this(new BufferInputStream(data));
        limit = data.length;
    }

    public CodedInputStream(byte[] data) {
        this(new BufferInputStream(data));
        limit = data.length;
    }

    /**
     * Attempt to read a field tag, returning zero if we have reached EOF.
     * Protocol message parsers use this to read tags, since a protocol message
     * may legally end wherever a tag occurs, and zero is not a valid tag
     * number.
     */
    public int readTag() throws IOException {
        if( pos >= limit ) {
            lastTag=0;
            return 0;
        }
        try {
            lastTag = readRawVarint32();
            if (lastTag == 0) {
                // If we actually read zero, that's not a valid tag.
                throw InvalidProtocolBufferException.invalidTag();
            }
            return lastTag;
        } catch (EOFException e) {
            lastTag=0;
            return 0;
        }
    }

    
    /**
     * Verifies that the last call to readTag() returned the given tag value.
     * This is used to verify that a nested group ended with the correct end
     * tag.
     * 
     * @throws InvalidProtocolBufferException
     *             {@code value} does not match the last tag.
     */
    public void checkLastTagWas(int value) throws InvalidProtocolBufferException {
        if (lastTag != value) {
            throw InvalidProtocolBufferException.invalidEndTag();
        }
    }

    /**
     * Reads and discards a single field, given its tag value.
     * 
     * @return {@code false} if the tag is an endgroup tag, in which case
     *         nothing is skipped. Otherwise, returns {@code true}.
     */
    public boolean skipField(int tag) throws IOException {
        switch (WireFormat.getTagWireType(tag)) {
        case WireFormat.WIRETYPE_VARINT:
            readInt32();
            return true;
        case WireFormat.WIRETYPE_FIXED64:
            readRawLittleEndian64();
            return true;
        case WireFormat.WIRETYPE_LENGTH_DELIMITED:
            skipRawBytes(readRawVarint32());
            return true;
        case WireFormat.WIRETYPE_START_GROUP:
            skipMessage();
            checkLastTagWas(WireFormat.makeTag(WireFormat.getTagFieldNumber(tag), WireFormat.WIRETYPE_END_GROUP));
            return true;
        case WireFormat.WIRETYPE_END_GROUP:
            return false;
        case WireFormat.WIRETYPE_FIXED32:
            readRawLittleEndian32();
            return true;
        default:
            throw InvalidProtocolBufferException.invalidWireType();
        }
    }

    /**
     * Reads and discards an entire message. This will read either until EOF or
     * until an endgroup tag, whichever comes first.
     */
    public void skipMessage() throws IOException {
        while (true) {
            int tag = readTag();
            if (tag == 0 || !skipField(tag))
                return;
        }
    }

    // -----------------------------------------------------------------

    /** Read a {@code double} field value from the stream. */
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readRawLittleEndian64());
    }

    /** Read a {@code float} field value from the stream. */
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readRawLittleEndian32());
    }

    /** Read a {@code uint64} field value from the stream. */
    public long readUInt64() throws IOException {
        return readRawVarint64();
    }

    /** Read an {@code int64} field value from the stream. */
    public long readInt64() throws IOException {
        return readRawVarint64();
    }

    /** Read an {@code int32} field value from the stream. */
    public int readInt32() throws IOException {
        return readRawVarint32();
    }

    /** Read a {@code fixed64} field value from the stream. */
    public long readFixed64() throws IOException {
        return readRawLittleEndian64();
    }

    /** Read a {@code fixed32} field value from the stream. */
    public int readFixed32() throws IOException {
        return readRawLittleEndian32();
    }

    /** Read a {@code bool} field value from the stream. */
    public boolean readBool() throws IOException {
        return readRawVarint32() != 0;
    }

    /** Read a {@code string} field value from the stream. */
    public String readString() throws IOException {
        int size = readRawVarint32();
        Buffer data = readRawBytes(size);
        return new String(data.data, data.offset, data.length, "UTF-8");
    }

    /** Read a {@code bytes} field value from the stream. */
    public Buffer readBytes() throws IOException {
        int size = readRawVarint32();
        return readRawBytes(size);
    }

    /** Read a {@code uint32} field value from the stream. */
    public int readUInt32() throws IOException {
        return readRawVarint32();
    }

    /**
     * Read an enum field value from the stream. Caller is responsible for
     * converting the numeric value to an actual enum.
     */
    public int readEnum() throws IOException {
        return readRawVarint32();
    }

    /** Read an {@code sfixed32} field value from the stream. */
    public int readSFixed32() throws IOException {
        return readRawLittleEndian32();
    }

    /** Read an {@code sfixed64} field value from the stream. */
    public long readSFixed64() throws IOException {
        return readRawLittleEndian64();
    }

    /** Read an {@code sint32} field value from the stream. */
    public int readSInt32() throws IOException {
        return decodeZigZag32(readRawVarint32());
    }

    /** Read an {@code sint64} field value from the stream. */
    public long readSInt64() throws IOException {
        return decodeZigZag64(readRawVarint64());
    }

    // =================================================================

    /**
     * Read a raw Varint from the stream. If larger than 32 bits, discard the
     * upper bits.
     */
    public int readRawVarint32() throws IOException {
        byte tmp = readRawByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = readRawByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = readRawByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = readRawByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = readRawByte()) << 28;
                    if (tmp < 0) {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++) {
                            if (readRawByte() >= 0)
                                return result;
                        }
                        throw InvalidProtocolBufferException.malformedVarint();
                    }
                }
            }
        }
        return result;
    }

    /** Read a raw Varint from the stream. */
    public long readRawVarint64() throws IOException {
        int shift = 0;
        long result = 0;
        while (shift < 64) {
            byte b = readRawByte();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
        }
        throw InvalidProtocolBufferException.malformedVarint();
    }

    /** Read a 32-bit little-endian integer from the stream. */
    public int readRawLittleEndian32() throws IOException {
        byte b1 = readRawByte();
        byte b2 = readRawByte();
        byte b3 = readRawByte();
        byte b4 = readRawByte();
        return (((int) b1 & 0xff)) | (((int) b2 & 0xff) << 8) | (((int) b3 & 0xff) << 16) | (((int) b4 & 0xff) << 24);
    }

    /** Read a 64-bit little-endian integer from the stream. */
    public long readRawLittleEndian64() throws IOException {
        byte b1 = readRawByte();
        byte b2 = readRawByte();
        byte b3 = readRawByte();
        byte b4 = readRawByte();
        byte b5 = readRawByte();
        byte b6 = readRawByte();
        byte b7 = readRawByte();
        byte b8 = readRawByte();
        return (((long) b1 & 0xff)) | (((long) b2 & 0xff) << 8) | (((long) b3 & 0xff) << 16) | (((long) b4 & 0xff) << 24) | (((long) b5 & 0xff) << 32) | (((long) b6 & 0xff) << 40) | (((long) b7 & 0xff) << 48) | (((long) b8 & 0xff) << 56);
    }

    /**
     * Decode a ZigZag-encoded 32-bit value. ZigZag encodes signed integers into
     * values that can be efficiently encoded with varint. (Otherwise, negative
     * values must be sign-extended to 64 bits to be varint encoded, thus always
     * taking 10 bytes on the wire.)
     * 
     * @param n
     *            An unsigned 32-bit integer, stored in a signed int because
     *            Java has no explicit unsigned support.
     * @return A signed 32-bit integer.
     */
    public static int decodeZigZag32(int n) {
        return (n >>> 1) ^ -(n & 1);
    }

    /**
     * Decode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into
     * values that can be efficiently encoded with varint. (Otherwise, negative
     * values must be sign-extended to 64 bits to be varint encoded, thus always
     * taking 10 bytes on the wire.)
     * 
     * @param n
     *            An unsigned 64-bit integer, stored in a signed int because
     *            Java has no explicit unsigned support.
     * @return A signed 64-bit integer.
     */
    public static long decodeZigZag64(long n) {
        return (n >>> 1) ^ -(n & 1);
    }   

    /**
     * Read one byte from the input.
     * 
     * @throws InvalidProtocolBufferException
     *             The end of the stream or the current limit was reached.
     */
    public byte readRawByte() throws IOException {
        if( pos >= limit ) {
            throw new EOFException();
        }
        int rc = in.read();
        if( rc < 0 ) {
            throw new EOFException();
        }
        pos++;
        return (byte)( rc & 0xFF); 
    }

    /**
     * Read a fixed size of bytes from the input.
     * 
     * @throws InvalidProtocolBufferException
     *             The end of the stream or the current limit was reached.
     */
    public Buffer readRawBytes(int size) throws IOException {
        if( size == 0) {
            return new Buffer(new byte[]{});
        }
        if( this.pos+size > limit ) {
            throw new EOFException();
        }
        
        // If the underlying stream is a ByteArrayInputStream
        // then we can avoid an array copy.
        if( bis!=null ) {
            Buffer rc = bis.readBuffer(size);
            if( rc==null || rc.getLength() < size ) {
                throw new EOFException();
            }
            this.pos += rc.getLength();
            return rc;
        }

        // Otherwise we, have to do it the old fasioned way
        byte[] rc = new byte[size];
        int c;
        int pos=0;
        while( pos < size ) {
            c = in.read(rc, pos, size-pos);
            if( c < 0 ) {
                throw new EOFException();
            }
            this.pos += c;
            pos += c;
        }
        
        return new Buffer(rc);
    }

    /**
     * Reads and discards {@code size} bytes.
     * 
     * @throws InvalidProtocolBufferException
     *             The end of the stream or the current limit was reached.
     */
    public void skipRawBytes(int size) throws IOException {
        int pos = 0;
        while (pos < size) {
            int n = (int) in.skip(size - pos);
            pos += n;
        }
    }

    public int pushLimit(int limit) {
        int rc = this.limit;
        this.limit = pos+limit;
        return rc;
    }

    public void popLimit(int limit) {
        this.limit = limit;
    }
  
}
