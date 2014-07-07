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
package org.apache.activemq.store.kahadb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.store.kahadb.disk.journal.Location;
import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.Transaction;

/**
 * Interface for the store meta data used to hold the index value and other needed
 * information to manage a KahaDB store instance.
 */
public interface KahaDBMetaData<T> {

    /**
     * Indicates that this meta data instance has been opened and is active.
     */
    public static final int OPEN_STATE = 2;

    /**
     * Indicates that this meta data instance has been closed and is no longer active.
     */
    public static final int CLOSED_STATE = 1;

    /**
     * Gets the Page in the store PageFile where the KahaDBMetaData instance is stored.
     *
     * @return the Page to use to start access the KahaDBMetaData instance.
     */
    Page<T> getPage();

    /**
     * Sets the Page instance used to load and store the KahaDBMetaData instance.
     *
     * @param page
     *        the new Page value to use.
     */
    void setPage(Page<T> page);

    /**
     * Gets the state flag of this meta data instance.
     *
     *  @return the current state value for this instance.
     */
    int getState();

    /**
     * Sets the current value of the state flag.
     *
     * @param value
     *        the new value to assign to the state flag.
     */
    void setState(int value);

    /**
     * Returns the Journal Location value that indicates that last recorded update
     * that was successfully performed for this KahaDB store implementation.
     *
     * @return the location of the last successful update location.
     */
    Location getLastUpdateLocation();

    /**
     * Updates the value of the last successful update.
     *
     * @param location
     *        the new value to assign the last update location field.
     */
    void setLastUpdateLocation(Location location);

    /**
     * For a newly created KahaDBMetaData instance this method is called to allow
     * the instance to create all of it's internal indices and other state data.
     *
     * @param tx
     *        the Transaction instance under which the operation is executed.
     *
     * @throws IOException if an error occurs while creating the meta data structures.
     */
    void initialize(Transaction tx) throws IOException;

    /**
     * Instructs this object to load its internal data structures from the KahaDB PageFile
     * and prepare itself for use.
     *
     * @param tx
     *        the Transaction instance under which the operation is executed.
     *
     * @throws IOException if an error occurs while creating the meta data structures.
     */
    void load(Transaction tx) throws IOException;

    /**
     * Reads the serialized for of this object from the KadaDB PageFile and prepares it
     * for use.  This method does not need to perform a full load of the meta data structures
     * only read in the information necessary to load them from the PageFile on a call to the
     * load method.
     *
     * @param in
     *        the DataInput instance used to read this objects serialized form.
     *
     * @throws IOException if an error occurs while reading the serialized form.
     */
    void read(DataInput in) throws IOException;

    /**
     * Writes the object into a serialized form which can be read back in again using the
     * read method.
     *
     * @param out
     *        the DataOutput instance to use to write the current state to a serialized form.
     *
     * @throws IOException if an error occurs while serializing this instance.
     */
    void write(DataOutput out) throws IOException;

}
