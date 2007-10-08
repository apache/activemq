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

import java.io.IOException;
import java.util.Set;

/**
 * A Store is holds persistent containers
 * 
 * @version $Revision: 1.2 $
 */
public interface Store {
    /**
     * Defauly container name
     */
    String DEFAULT_CONTAINER_NAME = "kaha";

    /**
     * Byte Marshaller
     */
    Marshaller BYTES_MARSHALLER = new BytesMarshaller();

    /**
     * Object Marshaller
     */
    Marshaller OBJECT_MARSHALLER = new ObjectMarshaller();

    /**
     * String Marshaller
     */
    Marshaller STRING_MARSHALLER = new StringMarshaller();

    /**
     * Command Marshaller
     */
    Marshaller COMMAND_MARSHALLER = new CommandMarshaller();

    /**
     * close the store
     * 
     * @throws IOException
     */
    void close() throws IOException;

    /**
     * Force all writes to disk
     * 
     * @throws IOException
     */
    void force() throws IOException;

    /**
     * empty all the contents of the store
     * 
     * @throws IOException
     */
    void clear() throws IOException;

    /**
     * delete the store
     * 
     * @return true if the delete was successful
     * @throws IOException
     */
    boolean delete() throws IOException;

    /**
     * Checks if a MapContainer exists in the default container
     * 
     * @param id
     * @return new MapContainer
     * @throws IOException
     */
    boolean doesMapContainerExist(Object id) throws IOException;

    /**
     * Checks if a MapContainer exists in the named container
     * 
     * @param id
     * @param containerName
     * @return new MapContainer
     * @throws IOException
     */
    boolean doesMapContainerExist(Object id, String containerName) throws IOException;

    /**
     * Get a MapContainer with the given id - the MapContainer is created if
     * needed
     * 
     * @param id
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    MapContainer getMapContainer(Object id) throws IOException;

    /**
     * Get a MapContainer with the given id - the MapContainer is created if
     * needed
     * 
     * @param id
     * @param containerName
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    MapContainer getMapContainer(Object id, String containerName) throws IOException;

    /**
     * Get a MapContainer with the given id - the MapContainer is created if
     * needed
     * 
     * @param id
     * @param containerName
     * @param persistentIndex
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    MapContainer getMapContainer(Object id, String containerName, boolean persistentIndex) throws IOException;

    /**
     * delete a container from the default container
     * 
     * @param id
     * @throws IOException
     */
    void deleteMapContainer(Object id) throws IOException;

    /**
     * delete a MapContainer from the name container
     * 
     * @param id
     * @param containerName
     * @throws IOException
     */
    void deleteMapContainer(Object id, String containerName) throws IOException;

    /**
     * Delete Map container
     * 
     * @param id
     * @throws IOException
     */
    void deleteMapContainer(ContainerId id) throws IOException;

    /**
     * Get a Set of call MapContainer Ids
     * 
     * @return the set of ids
     * @throws IOException
     */
    Set<ContainerId> getMapContainerIds() throws IOException;

    /**
     * Checks if a ListContainer exists in the default container
     * 
     * @param id
     * @return new MapContainer
     * @throws IOException
     */
    boolean doesListContainerExist(Object id) throws IOException;

    /**
     * Checks if a ListContainer exists in the named container
     * 
     * @param id
     * @param containerName
     * @return new MapContainer
     * @throws IOException
     */
    boolean doesListContainerExist(Object id, String containerName) throws IOException;

    /**
     * Get a ListContainer with the given id and creates it if it doesn't exist
     * 
     * @param id
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    ListContainer getListContainer(Object id) throws IOException;

    /**
     * Get a ListContainer with the given id and creates it if it doesn't exist
     * 
     * @param id
     * @param containerName
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    ListContainer getListContainer(Object id, String containerName) throws IOException;

    /**
     * Get a ListContainer with the given id and creates it if it doesn't exist
     * 
     * @param id
     * @param containerName
     * @param persistentIndex
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    ListContainer getListContainer(Object id, String containerName, boolean persistentIndex) throws IOException;

    /**
     * delete a ListContainer from the default container
     * 
     * @param id
     * @throws IOException
     */
    void deleteListContainer(Object id) throws IOException;

    /**
     * delete a ListContainer from the named container
     * 
     * @param id
     * @param containerName
     * @throws IOException
     */
    void deleteListContainer(Object id, String containerName) throws IOException;

    /**
     * delete a list container
     * 
     * @param id
     * @throws IOException
     */
    void deleteListContainer(ContainerId id) throws IOException;

    /**
     * Get a Set of call ListContainer Ids
     * 
     * @return the set of ids
     * @throws IOException
     */
    Set<ContainerId> getListContainerIds() throws IOException;

    /**
     * @return the maxDataFileLength
     */
    long getMaxDataFileLength();

    /**
     * @param maxDataFileLength the maxDataFileLength to set
     */
    void setMaxDataFileLength(long maxDataFileLength);


    /**
     * @return true if the store has been initialized
     */
    boolean isInitialized();
    
    /**
     * @return the amount of disk space the store is occupying
     */
    long size();
    
    public boolean isPersistentIndex();
    
	public void setPersistentIndex(boolean persistentIndex);
}
