//Protocol Buffers - Google's data interchange format
//Copyright 2008 Google Inc.
//http://code.google.com/p/protobuf/
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
package org.apache.activemq.protobuf;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Encodes and writes protocol message fields.
 * 
 * <p>
 * This class contains two kinds of methods: methods that write specific
 * protocol message constructs and field types (e.g. {@link #writeTag} and
 * {@link #writeInt32}) and methods that write low-level values (e.g.
 * {@link #writeRawVarint32} and {@link #writeRawBytes}). If you are writing
 * encoded protocol messages, you should use the former methods, but if you are
 * writing some other format of your own design, use the latter.
 * 
 * <p>
 * This class is totally unsynchronized.
 * 
 * @author kneton@google.com Kenton Varda
 */
public final class CodedOutputStream extends FilterOutputStream {

    private BufferOutputStream bos;

    public CodedOutputStream(OutputStream os) {
        super(os);
        if( os instanceof BufferOutputStream ) {
            bos = (BufferOutputStream)os;
        }
    }
    
    public CodedOutputStream(byte[] data) {
        super(new BufferOutputStream(data));
    }
    
    public CodedOutputStream(Buffer data) {
        super(new BufferOutputStream(data));
    }

    // -----------------------------------------------------------------

    /** Write a {@code double} field, including tag, to the stream. */
    public void writeDouble(int fieldNumber, double value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED64);
        writeRawLittleEndian64(Double.doubleToRawLongBits(value));
    }

    /** Write a {@code float} field, including tag, to the stream. */
    public void writeFloat(int fieldNumber, float value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED32);
        writeRawLittleEndian32(Float.floatToRawIntBits(value));
    }

    /** Write a {@code uint64} field, including tag, to the stream. */
    public void writeUInt64(int fieldNumber, long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawVarint64(value);
    }

    /** Write an {@code int64} field, including tag, to the stream. */
    public void writeInt64(int fieldNumber, long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawVarint64(value);
    }

    /** Write an {@code int32} field, including tag, to the stream. */
    public void writeInt32(int fieldNumber, int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        if (value >= 0) {
            writeRawVarint32(value);
        } else {
            // Must sign-extend.
            writeRawVarint64(value);
        }
    }

    /** Write a {@code fixed64} field, including tag, to the stream. */
    public void writeFixed64(int fieldNumber, long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED64);
        writeRawLittleEndian64(value);
    }

    /** Write a {@code fixed32} field, including tag, to the stream. */
    public void writeFixed32(int fieldNumber, int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED32);
        writeRawLittleEndian32(value);
    }

    /** Write a {@code bool} field, including tag, to the stream. */
    public void writeBool(int fieldNumber, boolean value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawByte(value ? 1 : 0);
    }

    /** Write a {@code string} field, including tag, to the stream. */
    public void writeString(int fieldNumber, String value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        // Unfortunately there does not appear to be any way to tell Java to
        // encode
        // UTF-8 directly into our buffer, so we have to let it create its own
        // byte
        // array and then copy.
        byte[] bytes = value.getBytes("UTF-8");
        writeRawVarint32(bytes.length);
        writeRawBytes(bytes);
    }

    /** Write a {@code bytes} field, including tag, to the stream. */
    public void writeBytes(int fieldNumber, Buffer value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        writeRawVarint32(value.length);
        writeRawBytes(value.data, value.offset, value.length);
    }

    /** Write a {@code uint32} field, including tag, to the stream. */
    public void writeUInt32(int fieldNumber, int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawVarint32(value);
    }

    /**
     * Write an enum field, including tag, to the stream. Caller is responsible
     * for converting the enum value to its numeric value.
     */
    public void writeEnum(int fieldNumber, int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawVarint32(value);
    }

    /** Write an {@code sfixed32} field, including tag, to the stream. */
    public void writeSFixed32(int fieldNumber, int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED32);
        writeRawLittleEndian32(value);
    }

    /** Write an {@code sfixed64} field, including tag, to the stream. */
    public void writeSFixed64(int fieldNumber, long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED64);
        writeRawLittleEndian64(value);
    }

    /** Write an {@code sint32} field, including tag, to the stream. */
    public void writeSInt32(int fieldNumber, int value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawVarint32(encodeZigZag32(value));
    }

    /** Write an {@code sint64} field, including tag, to the stream. */
    public void writeSInt64(int fieldNumber, long value) throws IOException {
        writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
        writeRawVarint64(encodeZigZag64(value));
    }

    // =================================================================

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * double} field, including tag.
     */
    public static int computeDoubleSize(int fieldNumber, double value) {
        return computeTagSize(fieldNumber) + LITTLE_ENDIAN_64_SIZE;
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * float} field, including tag.
     */
    public static int computeFloatSize(int fieldNumber, float value) {
        return computeTagSize(fieldNumber) + LITTLE_ENDIAN_32_SIZE;
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * uint64} field, including tag.
     */
    public static int computeUInt64Size(int fieldNumber, long value) {
        return computeTagSize(fieldNumber) + computeRawVarint64Size(value);
    }

    /**
     * Compute the number of bytes that would be needed to encode an {@code
     * int64} field, including tag.
     */
    public static int computeInt64Size(int fieldNumber, long value) {
        return computeTagSize(fieldNumber) + computeRawVarint64Size(value);
    }

    /**
     * Compute the number of bytes that would be needed to encode an {@code
     * int32} field, including tag.
     */
    public static int computeInt32Size(int fieldNumber, int value) {
        if (value >= 0) {
            return computeTagSize(fieldNumber) + computeRawVarint32Size(value);
        } else {
            // Must sign-extend.
            return computeTagSize(fieldNumber) + 10;
        }
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * fixed64} field, including tag.
     */
    public static int computeFixed64Size(int fieldNumber, long value) {
        return computeTagSize(fieldNumber) + LITTLE_ENDIAN_64_SIZE;
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * fixed32} field, including tag.
     */
    public static int computeFixed32Size(int fieldNumber, int value) {
        return computeTagSize(fieldNumber) + LITTLE_ENDIAN_32_SIZE;
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code bool}
     * field, including tag.
     */
    public static int computeBoolSize(int fieldNumber, boolean value) {
        return computeTagSize(fieldNumber) + 1;
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * string} field, including tag.
     */
    public static int computeStringSize(int fieldNumber, String value) {
        try {
            byte[] bytes = value.getBytes("UTF-8");
            return computeTagSize(fieldNumber) + computeRawVarint32Size(bytes.length) + bytes.length;
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException("UTF-8 not supported.", e);
        }
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * bytes} field, including tag.
     */
    public static int computeBytesSize(int fieldNumber, Buffer value) {
        return computeTagSize(fieldNumber) + computeRawVarint32Size(value.length) + value.length;
    }

    /**
     * Compute the number of bytes that would be needed to encode a {@code
     * uint32} field, including tag.
     */
    public static int computeUInt32Size(int fieldNumber, int value) {
        return computeTagSize(fieldNumber) + computeRawVarint32Size(value);
    }

    /**
     * Compute the number of bytes that would be needed to encode an enum field,
     * including tag. Caller is responsible for converting the enum value to its
     * numeric value.
     */
    public static int computeEnumSize(int fieldNumber, int value) {
        return computeTagSize(fieldNumber) + computeRawVarint32Size(value);
    }

    /**
     * Compute the number of bytes that would be needed to encode an {@code
     * sfixed32} field, including tag.
     */
    public static int computeSFixed32Size(int fieldNumber, int value) {
        return computeTagSize(fieldNumber) + LITTLE_ENDIAN_32_SIZE;
    }

    /**
     * Compute the number of bytes that would be needed to encode an {@code
     * sfixed64} field, including tag.
     */
    public static int computeSFixed64Size(int fieldNumber, long value) {
        return computeTagSize(fieldNumber) + LITTLE_ENDIAN_64_SIZE;
    }

    /**
     * Compute the number of bytes that would be needed to encode an {@code
     * sint32} field, including tag.
     */
    public static int computeSInt32Size(int fieldNumber, int value) {
        return computeTagSize(fieldNumber) + computeRawVarint32Size(encodeZigZag32(value));
    }

    /**
     * Compute the number of bytes that would be needed to encode an {@code
     * sint64} field, including tag.
     */
    public static int computeSInt64Size(int fieldNumber, long value) {
        return computeTagSize(fieldNumber) + computeRawVarint64Size(encodeZigZag64(value));
    }

    /** Write a single byte. */
    public void writeRawByte(byte value) throws IOException {
        out.write(value);
    }

    /** Write a single byte, represented by an integer value. */
    public void writeRawByte(int value) throws IOException {
        writeRawByte((byte) value);
    }

    /** Write an array of bytes. */
    public void writeRawBytes(byte[] value) throws IOException {
        writeRawBytes(value, 0, value.length);
    }

    /** Write part of an array of bytes. */
    public void writeRawBytes(byte[] value, int offset, int length) throws IOException {
        out.write(value, offset, length);
    }

    public void writeRawBytes(Buffer data) throws IOException {
        out.write(data.data, data.offset, data.length);
    }

    /** Encode and write a tag. */
    public void writeTag(int fieldNumber, int wireType) throws IOException {
        writeRawVarint32(WireFormat.makeTag(fieldNumber, wireType));
    }

    /** Compute the number of bytes that would be needed to encode a tag. */
    public static int computeTagSize(int fieldNumber) {
        return computeRawVarint32Size(WireFormat.makeTag(fieldNumber, 0));
    }

    /**
     * Encode and write a varint. {@code value} is treated as unsigned, so it
     * won't be sign-extended if negative.
     */
    public void writeRawVarint32(int value) throws IOException {
        while (true) {
            if ((value & ~0x7F) == 0) {
                writeRawByte(value);
                return;
            } else {
                writeRawByte((value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    /**
     * Compute the number of bytes that would be needed to encode a varint.
     * {@code value} is treated as unsigned, so it won't be sign-extended if
     * negative.
     */
    public static int computeRawVarint32Size(int value) {
        if ((value & (0xffffffff << 7)) == 0)
            return 1;
        if ((value & (0xffffffff << 14)) == 0)
            return 2;
        if ((value & (0xffffffff << 21)) == 0)
            return 3;
        if ((value & (0xffffffff << 28)) == 0)
            return 4;
        return 5;
    }

    /** Encode and write a varint. */
    public void writeRawVarint64(long value) throws IOException {
        while (true) {
            if ((value & ~0x7FL) == 0) {
                writeRawByte((int) value);
                return;
            } else {
                writeRawByte(((int) value & 0x7F) | 0x80);
                value >>>= 7;
            }
        }
    }

    /** Compute the number of bytes that would be needed to encode a varint. */
    public static int computeRawVarint64Size(long value) {
        if ((value & (0xffffffffffffffffL << 7)) == 0)
            return 1;
        if ((value & (0xffffffffffffffffL << 14)) == 0)
            return 2;
        if ((value & (0xffffffffffffffffL << 21)) == 0)
            return 3;
        if ((value & (0xffffffffffffffffL << 28)) == 0)
            return 4;
        if ((value & (0xffffffffffffffffL << 35)) == 0)
            return 5;
        if ((value & (0xffffffffffffffffL << 42)) == 0)
            return 6;
        if ((value & (0xffffffffffffffffL << 49)) == 0)
            return 7;
        if ((value & (0xffffffffffffffffL << 56)) == 0)
            return 8;
        if ((value & (0xffffffffffffffffL << 63)) == 0)
            return 9;
        return 10;
    }

    /** Write a little-endian 32-bit integer. */
    public void writeRawLittleEndian32(int value) throws IOException {
        writeRawByte((value) & 0xFF);
        writeRawByte((value >> 8) & 0xFF);
        writeRawByte((value >> 16) & 0xFF);
        writeRawByte((value >> 24) & 0xFF);
    }

    public static final int LITTLE_ENDIAN_32_SIZE = 4;

    /** Write a little-endian 64-bit integer. */
    public void writeRawLittleEndian64(long value) throws IOException {
        writeRawByte((int) (value) & 0xFF);
        writeRawByte((int) (value >> 8) & 0xFF);
        writeRawByte((int) (value >> 16) & 0xFF);
        writeRawByte((int) (value >> 24) & 0xFF);
        writeRawByte((int) (value >> 32) & 0xFF);
        writeRawByte((int) (value >> 40) & 0xFF);
        writeRawByte((int) (value >> 48) & 0xFF);
        writeRawByte((int) (value >> 56) & 0xFF);
    }

    public static final int LITTLE_ENDIAN_64_SIZE = 8;

    /**
     * Encode a ZigZag-encoded 32-bit value. ZigZag encodes signed integers into
     * values that can be efficiently encoded with varint. (Otherwise, negative
     * values must be sign-extended to 64 bits to be varint encoded, thus always
     * taking 10 bytes on the wire.)
     * 
     * @param n
     *            A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored in a signed int because Java
     *         has no explicit unsigned support.
     */
    public static int encodeZigZag32(int n) {
        // Note: the right-shift must be arithmetic
        return (n << 1) ^ (n >> 31);
    }

    /**
     * Encode a ZigZag-encoded 64-bit value. ZigZag encodes signed integers into
     * values that can be efficiently encoded with varint. (Otherwise, negative
     * values must be sign-extended to 64 bits to be varint encoded, thus always
     * taking 10 bytes on the wire.)
     * 
     * @param n
     *            A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because Java
     *         has no explicit unsigned support.
     */
    public static long encodeZigZag64(long n) {
        // Note: the right-shift must be arithmetic
        return (n << 1) ^ (n >> 63);
    }

    public void checkNoSpaceLeft() {
    }

    public Buffer getNextBuffer(int size) throws IOException {
        if( bos==null ) {
            return null;
        }
        return bos.getNextBuffer(size);
    }

}
