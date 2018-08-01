/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp.message;

import java.util.Arrays;
import java.util.Collection;

import org.apache.qpid.proton.codec.DecoderImpl;
import org.apache.qpid.proton.codec.EncoderImpl;
import org.apache.qpid.proton.codec.EncodingCodes;
import org.apache.qpid.proton.codec.PrimitiveType;
import org.apache.qpid.proton.codec.PrimitiveTypeEncoding;
import org.apache.qpid.proton.codec.TypeEncoding;
import org.apache.qpid.proton.codec.WritableBuffer;
import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * AMQP Type used to allow to proton-j codec to deal with UTF8Buffer types as if
 * they were String elements.
 */
public class UTF8BufferType implements PrimitiveType<UTF8Buffer> {

    private final UTF8BufferEncoding largeBufferEncoding;
    private final UTF8BufferEncoding smallBufferEncoding;

    public UTF8BufferType(EncoderImpl encoder, DecoderImpl decoder) {
        this.largeBufferEncoding = new LargeUTF8BufferEncoding(encoder, decoder);
        this.smallBufferEncoding = new SmallUTF8BufferEncoding(encoder, decoder);
    }

    @Override
    public Class<UTF8Buffer> getTypeClass() {
        return UTF8Buffer.class;
    }

    @Override
    public PrimitiveTypeEncoding<UTF8Buffer> getEncoding(UTF8Buffer value) {
        return value.getLength() <= 255 ? smallBufferEncoding : largeBufferEncoding;
    }

    @Override
    public PrimitiveTypeEncoding<UTF8Buffer> getCanonicalEncoding() {
        return largeBufferEncoding;
    }

    @Override
    public Collection<? extends PrimitiveTypeEncoding<UTF8Buffer>> getAllEncodings() {
        return Arrays.asList(smallBufferEncoding, largeBufferEncoding);
    }

    @Override
    public void write(UTF8Buffer value) {
        final TypeEncoding<UTF8Buffer> encoding = getEncoding(value);
        encoding.writeConstructor();
        encoding.writeValue(value);
    }

    public abstract class UTF8BufferEncoding implements PrimitiveTypeEncoding<UTF8Buffer> {

        private final EncoderImpl encoder;
        private final DecoderImpl decoder;

        public UTF8BufferEncoding(EncoderImpl encoder, DecoderImpl decoder) {
            this.encoder = encoder;
            this.decoder = decoder;
        }

        @Override
        public int getConstructorSize() {
            return 1;
        }

        @Override
        public boolean isFixedSizeVal() {
            return false;
        }

        @Override
        public boolean encodesJavaPrimitive() {
            return false;
        }

        /**
         * @return the number of bytes the size portion of the encoded value requires.
         */
        public abstract int getSizeBytes();

        @Override
        public void writeConstructor() {
            getEncoder().writeRaw(getEncodingCode());
        }

        @Override
        public void writeValue(UTF8Buffer value) {
            writeSize(value);
            WritableBuffer buffer = getEncoder().getBuffer();
            buffer.put(value.getData(), value.getOffset(), value.getLength());
        }

        /**
         * Write the size of the buffer using the appropriate type (byte or int) depending
         * on the encoding type being used.
         *
         * @param value
         *      The UTF8Buffer value that is being encoded.
         */
        public abstract void writeSize(UTF8Buffer value);

        @Override
        public int getValueSize(UTF8Buffer value) {
            return getSizeBytes() + value.getLength();
        }

        @Override
        public Class<UTF8Buffer> getTypeClass() {
            return UTF8Buffer.class;
        }

        @Override
        public PrimitiveType<UTF8Buffer> getType() {
            return UTF8BufferType.this;
        }

        @Override
        public boolean encodesSuperset(TypeEncoding<UTF8Buffer> encoding) {
            return (getType() == encoding.getType());
        }

        @Override
        public UTF8Buffer readValue() {
            throw new UnsupportedOperationException("No decoding to UTF8Buffer exists");
        }

        @Override
        public void skipValue() {
            throw new UnsupportedOperationException("No decoding to UTF8Buffer exists");
        }

        public DecoderImpl getDecoder() {
            return decoder;
        }

        public EncoderImpl getEncoder() {
            return encoder;
        }
    }

    public class LargeUTF8BufferEncoding extends UTF8BufferEncoding {

        public LargeUTF8BufferEncoding(EncoderImpl encoder, DecoderImpl decoder) {
            super(encoder, decoder);
        }

        @Override
        public byte getEncodingCode() {
            return EncodingCodes.STR32;
        }

        @Override
        public int getSizeBytes() {
            return Integer.BYTES;
        }

        @Override
        public void writeSize(UTF8Buffer value) {
            getEncoder().getBuffer().putInt(value.getLength());
        }
    }

    public class SmallUTF8BufferEncoding extends UTF8BufferEncoding {

        public SmallUTF8BufferEncoding(EncoderImpl encoder, DecoderImpl decoder) {
            super(encoder, decoder);
        }

        @Override
        public byte getEncodingCode() {
            return EncodingCodes.STR8;
        }

        @Override
        public int getSizeBytes() {
            return Byte.BYTES;
        }

        @Override
        public void writeSize(UTF8Buffer value) {
            getEncoder().getBuffer().put((byte) value.getLength());
        }
    }
}
