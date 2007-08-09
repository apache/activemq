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
package org.apache.activemq.memory;

/**
 * Defines the interface used to cache messages.
 * 
 * @version $Revision$
 */
public interface Cache {

    /**
     * Gets an object that was previously <code>put</code> into this object.
     * 
     * @param msgid
     * @return null if the object was not previously put or if the object has
     *         expired out of the cache.
     */
    Object get(Object key);

    /**
     * Puts an object into the cache.
     * 
     * @param messageID
     * @param message
     */
    Object put(Object key, Object value);

    /**
     * Removes an object from the cache.
     * 
     * @param messageID
     * @return the object associated with the key if it was still in the cache.
     */
    Object remove(Object key);

    /**
     * Lets a cache know it will not be used any further and that it can release
     * acquired resources
     */
    void close();

    /**
     * How big is the cache right now?
     * 
     * @return
     */
    int size();

}
