/**
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
package org.apache.activemq.util;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

public class RecoverableRandomAccessFile implements java.io.DataOutput, java.io.DataInput, java.io.Closeable {

    private static final boolean SKIP_METADATA_UPDATE =
        Boolean.getBoolean("org.apache.activemq.kahaDB.files.skipMetadataUpdate");

    RandomAccessFile raf;
    File file;
    String mode;
    final boolean isSkipMetadataUpdate;

    public RecoverableRandomAccessFile(File file, String mode, boolean skipMetadataUpdate) throws FileNotFoundException {
        this.file = file;
        this.mode = mode;
        raf = new RandomAccessFile(file, mode);
        isSkipMetadataUpdate = skipMetadataUpdate;
    }

    public RecoverableRandomAccessFile(File file, String mode) throws FileNotFoundException {
        this(file, mode, SKIP_METADATA_UPDATE);
    }

    public RecoverableRandomAccessFile(String name, String mode) throws FileNotFoundException {
        this(new File(name), mode);
    }

    public RandomAccessFile getRaf() throws IOException {
        if (raf == null) {
            raf = new RandomAccessFile(file, mode);
        }
        return raf;
    }

    protected void handleException() throws IOException {
        try {
            if (raf != null) {
                raf.close();
            }
        } catch (Throwable ignore) {
        } finally {
            raf = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (raf != null) {
            raf.close();
        }
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        try {
            getRaf().readFully(bytes);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void readFully(byte[] bytes, int i, int i2) throws IOException {
        try {
            getRaf().readFully(bytes, i, i2);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public int skipBytes(int i) throws IOException {
        try {
            return getRaf().skipBytes(i);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        try {
            return getRaf().readBoolean();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return getRaf().readByte();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public int readUnsignedByte() throws IOException {
        try {
            return getRaf().readUnsignedByte();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return getRaf().readShort();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public int readUnsignedShort() throws IOException {
        try {
            return getRaf().readUnsignedShort();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public char readChar() throws IOException {
        try {
            return getRaf().readChar();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return getRaf().readInt();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public long readLong() throws IOException {
        try {
            return getRaf().readLong();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public float readFloat() throws IOException {
        try {
            return getRaf().readFloat();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public double readDouble() throws IOException {
        try {
            return getRaf().readDouble();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public String readLine() throws IOException {
        try {
            return getRaf().readLine();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public String readUTF() throws IOException {
        try {
            return getRaf().readUTF();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void write(int i) throws IOException {
        try {
            getRaf().write(i);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        try {
            getRaf().write(bytes);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void write(byte[] bytes, int i, int i2) throws IOException {
        try {
            getRaf().write(bytes, i, i2);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        try {
            getRaf().writeBoolean(b);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeByte(int i) throws IOException {
        try {
            getRaf().writeByte(i);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeShort(int i) throws IOException {
        try {
            getRaf().writeShort(i);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeChar(int i) throws IOException {
        try {
            getRaf().writeChar(i);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeInt(int i) throws IOException {
        try {
            getRaf().writeInt(i);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeLong(long l) throws IOException {
        try {
            getRaf().writeLong(l);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeFloat(float v) throws IOException {
        try {
            getRaf().writeFloat(v);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeDouble(double v) throws IOException {
        try {
            getRaf().writeDouble(v);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeBytes(String s) throws IOException {
        try {
            getRaf().writeBytes(s);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        try {
            getRaf().writeChars(s);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    @Override
    public void writeUTF(String s) throws IOException {
        try {
            getRaf().writeUTF(s);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }


    //RAF methods
    public long length() throws IOException {
        try {
            return getRaf().length();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    public void setLength(long length) throws IOException {
        throw new IllegalStateException("File size is pre allocated");
    }

    public void seek(long pos) throws IOException {
        try {
            getRaf().seek(pos);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    public FileDescriptor getFD() throws IOException {
        try {
            return getRaf().getFD();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    public void sync() throws IOException {
        try {
            getRaf().getChannel().force(!isSkipMetadataUpdate);;
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    public FileChannel getChannel() throws IOException {
        try {
            return getRaf().getChannel();
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    public int read(byte[] b, int off, int len) throws IOException {
        try {
            return getRaf().read(b, off, len);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }

    public int read(byte[] b) throws IOException {
        try {
            return getRaf().read(b);
        } catch (IOException ioe) {
            handleException();
            throw ioe;
        }
    }
}
