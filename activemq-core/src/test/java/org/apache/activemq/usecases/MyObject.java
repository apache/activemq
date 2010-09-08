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

package org.apache.activemq.usecases;

import java.io.Serializable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.ObjectStreamException;
import java.util.concurrent.atomic.AtomicInteger;

public class MyObject implements Serializable {

    private String message;
    private AtomicInteger writeObjectCalled = new AtomicInteger(0);
    private AtomicInteger readObjectCalled = new AtomicInteger(0);
    private AtomicInteger readObjectNoDataCalled = new AtomicInteger(0);

    public MyObject(String message) {
        this.setMessage(message);
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        writeObjectCalled.incrementAndGet();
        out.defaultWriteObject();
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        readObjectCalled.incrementAndGet();        
    }

    private void readObjectNoData() throws ObjectStreamException {
        readObjectNoDataCalled.incrementAndGet();
    }

    public int getWriteObjectCalled() {
        return writeObjectCalled.get();
    }

    public int getReadObjectCalled() {
        return readObjectCalled.get();
    }

    public int getReadObjectNoDataCalled() {
        return readObjectNoDataCalled.get();
    }
}


