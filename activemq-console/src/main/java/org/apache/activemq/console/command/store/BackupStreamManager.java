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
package org.apache.activemq.console.command.store;

import org.apache.activemq.console.command.store.protobuf.*;
import org.apache.activemq.console.command.store.tar.TarEntry;
import org.apache.activemq.console.command.store.tar.TarOutputStream;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.MessageBuffer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author Matt Pavlovich <mattrpav@apache.org>
 */
public class BackupStreamManager {

    private final OutputStream target;
    private final int version;
    TarOutputStream stream;

    BackupStreamManager(OutputStream target, int version) throws IOException {
        this.target = target;
        this.version = version;
        stream = new TarOutputStream(new GZIPOutputStream(target));
        store("ver", new AsciiBuffer(""+version));
    }


    long seq = 0;

    public void finish() throws IOException {
        stream.close();
    }

    private void store(String ext, Buffer value) throws IOException {
        TarEntry entry = new TarEntry(seq + "." + ext);
        seq += 1;
        entry.setSize(value.getLength());
        stream.putNextEntry(entry);
        stream.write(value.getData());
        stream.closeEntry();
    }

    private void store(String ext, MessageBuffer<?,?> value) throws IOException {
        TarEntry entry = new TarEntry(seq + "." + ext);
        seq += 1;
        entry.setSize(value.serializedSizeFramed());
        stream.putNextEntry(entry);
        value.writeFramed(stream);
        stream.closeEntry();
    }


    public void store_queue(QueuePB value) throws IOException {
      store("que", value.toUnframedBuffer());
    }
    public void store_queue_entry(QueueEntryPB value) throws IOException {
      store("qen", value.toUnframedBuffer());
    }
    public void store_message(MessagePB value) throws IOException {
      store("msg", value.toUnframedBuffer());
    }
    public void store_map_entry(MapEntryPB value) throws IOException {
      store("map", value.toUnframedBuffer());
    }

}
