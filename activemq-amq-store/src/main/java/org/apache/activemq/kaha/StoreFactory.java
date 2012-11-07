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

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.kaha.impl.KahaStore;

/**
 * Factory for creating stores
 * 
 * 
 */
public final class StoreFactory {

    private StoreFactory() {
    }

    /**
     * open or create a Store
     * 
     * @param name
     * @param mode
     * @return the opened/created store
     * @throws IOException
     */
    public static Store open(String name, String mode) throws IOException {
        return new KahaStore(name, mode,new AtomicLong());
    }
    
    /**
     * Open or create a Store
     * 
     * @param directory
     * @param mode
     * @return
     * @throws IOException
     */
    public static Store open(File directory, String mode) throws IOException {
        return new KahaStore(directory, mode, new AtomicLong());
    }
    
    /**
     * open or create a Store
     * @param name
     * @param mode
     * @param size
     * @return the opened/created store
     * @throws IOException
     */
    public static Store open(String name, String mode, AtomicLong size) throws IOException {
        return new KahaStore(name, mode,size);
    }
    

    /**
     * Open or create a Store
     * 
     * @param directory
     * @param mode
     * @param size
     * @return
     * @throws IOException
     */
    public static Store open(File directory, String mode, AtomicLong size) throws IOException {
        return new KahaStore(directory, mode, size);
    }
    

    /**
     * Delete a database
     * 
     * @param name of the database
     * @return true if successful
     * @throws IOException
     */
    public static boolean delete(String name) throws IOException {
        KahaStore store = new KahaStore(name, "rw");
        return store.delete();
    }
    
    /**
     * Delete a database
     * 
     * @param directory
     * @return true if successful
     * @throws IOException
     */
    public static boolean delete(File directory) throws IOException {
        KahaStore store = new KahaStore(directory, "rw");
        return store.delete();
    }
}
