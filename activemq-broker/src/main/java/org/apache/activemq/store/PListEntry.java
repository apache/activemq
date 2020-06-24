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

import org.apache.activemq.util.ByteSequence;

public class PListEntry {

    private final ByteSequence byteSequence;
    private final String entry;
    private final Object locator;

    public PListEntry(String entry, ByteSequence bs, Object locator) {
        this.entry = entry;
        this.byteSequence = bs;
//IC see: https://issues.apache.org/jira/browse/AMQ-4215
        this.locator = locator;
    }

    public ByteSequence getByteSequence() {
        return this.byteSequence;
    }

    public String getId() {
        return this.entry;
    }

    public Object getLocator() {
//IC see: https://issues.apache.org/jira/browse/AMQ-4215
        return locator;
    }

    public PListEntry copy() {
        return new PListEntry(this.entry, this.byteSequence, locator);
    }
}
