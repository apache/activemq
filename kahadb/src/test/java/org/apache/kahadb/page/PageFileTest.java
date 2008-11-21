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
package org.apache.kahadb.page;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;

import org.apache.kahadb.util.StringMarshaller;

import junit.framework.TestCase;

public class PageFileTest extends TestCase {

    public void testCRUD() throws IOException {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        HashSet<String> expected = new HashSet<String>();

        // Insert some data into the page file.
        Transaction tx = pf.tx();
        for (int i = 0; i < 100; i++) {
            Page<String> page = tx.allocate();
            assertEquals(Page.PAGE_FREE_TYPE, page.getType());

            String t = "page:" + i;
            expected.add(t);
            page.set(t);
            tx.store(page, StringMarshaller.INSTANCE, false);
            tx.commit();
        }

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure they are still there..
        HashSet<String> actual = new HashSet<String>();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add(page.get());
        }
        assertEquals(expected, actual);

        // Remove the odd records..
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                break;
            }
            String t = "page:" + i;
            expected.remove(t);
        }
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            if (!expected.contains(page.get())) {
                tx.free(page);
            }
        }
        tx.commit();

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure the even records are still there..
        actual.clear();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add((String)page.get());
        }
        assertEquals(expected, actual);

        // Update the records...
        HashSet<String> t = expected;
        expected = new HashSet<String>();
        for (String s : t) {
            expected.add(s + ":updated");
        }
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            page.set(page.get() + ":updated");
            tx.store(page, StringMarshaller.INSTANCE, false);
        }
        tx.commit();

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure the updated records are still there..
        actual.clear();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add(page.get());
        }
        assertEquals(expected, actual);

        pf.unload();
    }

    public void testStreams() throws IOException {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        Transaction tx = pf.tx();
        Page page = tx.allocate();
        tx.commit();

        OutputStream pos = tx.openOutputStream(page, true);
        DataOutputStream os = new DataOutputStream(pos);
        for (int i = 0; i < 10000; i++) {
            os.writeUTF("Test string:" + i);
        }

        os.close();
        tx.commit();

        // Reload the page file.
        pf.unload();
        pf.load();
        tx = pf.tx();

        InputStream pis = tx.openInputStream(page);
        DataInputStream is = new DataInputStream(pis);
        for (int i = 0; i < 10000; i++) {
            assertEquals("Test string:" + i, is.readUTF());
        }
        assertEquals(-1, is.read());
        is.close();

        pf.unload();
    }

    public void testAddRollback() throws IOException {

        PageFile pf = new PageFile(new File("target/test-data"), getName());
        pf.delete();
        pf.load();

        HashSet<String> expected = new HashSet<String>();

        // Insert some data into the page file.
        Transaction tx = pf.tx();
        for (int i = 0; i < 100; i++) {
            Page<String> page = tx.allocate();
            assertEquals(Page.PAGE_FREE_TYPE, page.getType());

            String t = "page:" + i;
            page.set(t);
            tx.store(page, StringMarshaller.INSTANCE, false);

            // Rollback every other insert.
            if (i % 2 == 0) {
                expected.add(t);
                tx.commit();
            } else {
                tx.rollback();
            }

        }

        // Reload it...
        pf.unload();
        pf.load();
        tx = pf.tx();

        // Iterate it to make sure they are still there..
        HashSet<String> actual = new HashSet<String>();
        for (Page<String> page : tx) {
            tx.load(page, StringMarshaller.INSTANCE);
            actual.add(page.get());
        }
        assertEquals(expected, actual);
    }
}
