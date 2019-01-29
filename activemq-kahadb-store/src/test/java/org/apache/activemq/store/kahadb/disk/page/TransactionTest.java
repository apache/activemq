package org.apache.activemq.store.kahadb.disk.page;

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

import junit.framework.TestCase;
import org.apache.activemq.store.kahadb.disk.util.Marshaller;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransactionTest  extends TestCase {

    private static long NUMBER_OF_BYTES = 10485760L;

    static class TransactionTestMarshaller implements Marshaller<List<Byte>> {

        public static TransactionTestMarshaller INSTANCE = new TransactionTestMarshaller();

        @Override
        public void writePayload(final List<Byte> object, final DataOutput dataOut) throws IOException {
            for (Byte b : object) {
                dataOut.write(b);
            }
        }

        @Override
        public List<Byte> readPayload(final DataInput dataIn) throws IOException {
            List<Byte> result = new ArrayList<>();
            for (int i = 0; i < NUMBER_OF_BYTES; i++) {
                result.add(dataIn.readByte());
            }

            return result;
        }

        @Override
        public int getFixedSize() {
            return 0;
        }

        @Override
        public boolean isDeepCopySupported() {
            return false;
        }

        @Override
        public List<Byte> deepCopy(final List<Byte> source) {
            return new ArrayList<>(source);
        }
    }

    public void testDeleteTempFileWhenRollback() throws IOException {
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnablePageCaching(false);
        pf.load();
        System.setProperty("maxKahaDBTxSize", "" + (1024*1024));


        Transaction tx = pf.tx();
        Page<List<Byte>> page = tx.allocate();

        page.set(getBytes());

        tx.store(page, TransactionTestMarshaller.INSTANCE, true);

        File tempFile = tx.getTempFile();

        assertTrue(tempFile.exists());

        tx.rollback();
        pf.flush();

        assertFalse(tempFile.exists());
    }

    public void testHugeTransaction() throws IOException {
        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.setEnablePageCaching(false);
        pf.load();
        System.setProperty("maxKahaDBTxSize", "" + (1024*1024));


        Transaction tx = pf.tx();
        Page<List<Byte>> page = tx.allocate();

        List<Byte> bytes = getBytes();

        page.set(bytes);

        tx.store(page, TransactionTestMarshaller.INSTANCE, true);
        tx.commit();
        pf.flush();

        tx = pf.tx();

        page = tx.load(page.getPageId(), TransactionTestMarshaller.INSTANCE);

        for (int i = 0; i < NUMBER_OF_BYTES; i++) {
            assertEquals(bytes.get(i), page.get().get(i));
        }

    }

    private List<Byte> getBytes() {
        List<Byte> bytes = new ArrayList<>();
        byte b = 0;

        for (int i = 0; i < NUMBER_OF_BYTES; i++) {
            bytes.add(b++);
        }

        return bytes;
    }
}
