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
package org.apache.activemq.kaha;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Store test
 * 
 * @version $Revision: 1.2 $
 */
class Loader extends Thread {

    protected static final Log log = LogFactory.getLog(Loader.class);

    private String name;
    private Store store;
    private int count;
    private CountDownLatch start;
    private CountDownLatch stop;

    public Loader(String name, Store store, int count, CountDownLatch start, CountDownLatch stop) {
        this.name = name;
        this.store = store;
        this.count = count;
        this.start = start;
        this.stop = stop;
    }

    public void run() {
        try {
            start.countDown();
            start.await();
            Marshaller keyMarshaller = new StringMarshaller();
            Marshaller valueMarshaller = new BytesMarshaller();
            MapContainer container = store.getMapContainer(name, Store.DEFAULT_CONTAINER_NAME, true);

            container.setKeyMarshaller(keyMarshaller);
            container.setValueMarshaller(valueMarshaller);
            container.load();
            // set data
            Object value = getData(1024);
            long startTime = System.currentTimeMillis();
            long startLoad = System.currentTimeMillis();
            for (int i = 0; i < count; i++) {
                String key = "key:" + i;
                container.put(key, value);
            }
            long finishLoad = System.currentTimeMillis();
            long totalLoadTime = finishLoad - startLoad;
            log.info("name " + name + " load time = " + totalLoadTime + "(ms)");

            Set keys = container.keySet();
            long startExtract = System.currentTimeMillis();

            for (Iterator i = keys.iterator(); i.hasNext();) {
                byte[] data = (byte[])container.get(i.next());
            }
            long finishExtract = System.currentTimeMillis();
            long totalExtractTime = finishExtract - startExtract;
            log.info("name " + name + " extract time = " + totalExtractTime + "(ms)");

            long startRemove = System.currentTimeMillis();
            for (Iterator i = keys.iterator(); i.hasNext();) {
                container.remove(i.next());
            }
            long finishRemove = System.currentTimeMillis();
            long totalRemoveTime = finishRemove - startRemove;
            log.info("name " + name + " remove time = " + totalRemoveTime + "(ms)");
            // re-insert data of longer length
            startLoad = System.currentTimeMillis();
            value = getData(2048);
            for (int i = 0; i < count; i++) {
                // System.out.println(this + " Container size = " +
                // container.size());
                String key = "key:" + i;
                container.put(key, value);
            }
            finishLoad = System.currentTimeMillis();
            totalLoadTime = finishLoad - startLoad;
            log.info("name " + name + " 2nd load time = " + totalLoadTime + "(ms)");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            stop.countDown();
        }
    }

    byte[] getData(int size) {
        byte[] result = new byte[size];
        for (int i = 0; i < size; i++) {
            result[i] = 'a';
        }
        return result;
    }
}
