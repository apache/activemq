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
package org.apache.activemq.store;

import java.io.IOException;
import java.util.Iterator;

import org.apache.activemq.util.ByteSequence;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public interface PList {
    String getName();

    void destroy() throws IOException;

    Object addFirst(String id, ByteSequence bs) throws IOException;

    Object addLast(String id, ByteSequence bs) throws IOException;

    boolean remove(Object position) throws IOException;

    boolean isEmpty();

    PListIterator iterator() throws IOException;

    long size();

    long messageSize();

    public interface PListIterator extends Iterator<PListEntry> {
        void release();
    }
}
