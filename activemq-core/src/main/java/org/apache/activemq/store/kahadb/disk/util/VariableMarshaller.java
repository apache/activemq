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
package org.apache.activemq.store.kahadb.disk.util;

/**
 * Convenience base class for Marshaller implementations which do not deepCopy and
 * which use variable size encodings.
 * 
 * @author chirino
 * @param <T>
 */
abstract public class VariableMarshaller<T> implements Marshaller<T> {

    public int getFixedSize() {
        return -1;
    }

    public boolean isDeepCopySupported() {
        return false;
    }

    public T deepCopy(T source) {
        throw new UnsupportedOperationException();
    }

}
