/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
public interface Store{
    
    /**
     * Byte Marshaller
     */
    public final static Marshaller BytesMarshaller = new BytesMarshaller();
    
    /**
     * Object Marshaller
     */
    public final static Marshaller ObjectMarshaller = new ObjectMarshaller();
    
    /**
     * String Marshaller
     */
    public final static Marshaller StringMarshaller = new StringMarshaller();
    /**
     * close the store
     * 
     * @throws IOException
     */
    public void close() throws IOException;

    /**
     * Force all writes to disk
     * 
     * @throws IOException
     */
    public void force() throws IOException;

    /**
     * empty all the contents of the store
     * 
     * @throws IOException
     */
    public void clear() throws IOException;

    /**
     * delete the store
     * 
     * @return true if the delete was successful
     * @throws IOException
     */
    public boolean delete() throws IOException;

    /**
     * Checks if a MapContainer exists in the default container
     * 
     * @param id
     * @return new MapContainer
     * @throws IOException
     */
    public boolean doesMapContainerExist(Object id) throws IOException;
    
    /**
     * Checks if a MapContainer exists in the named container
     * 
     * @param id
     * @param containerName 
     * @return new MapContainer
     * @throws IOException
     */
    public boolean doesMapContainerExist(Object id,String containerName) throws IOException;

    /**
     * Get a MapContainer with the given id - the MapContainer is created if needed
     * 
     * @param id
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
     public MapContainer getMapContainer(Object id) throws IOException;

    /**
     * Get a MapContainer with the given id - the MapContainer is created if needed
     * 
     * @param id
     * @param containerName
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    public MapContainer getMapContainer(Object id,String containerName) throws IOException;
    
    /**
     * Get a MapContainer with the given id - the MapContainer is created if needed
     * 
     * @param id
     * @param containerName
     * @param indexType 
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    public MapContainer getMapContainer(Object id,String containerName,String indexType) throws IOException;

    /**
     * delete a container from the default container
     * 
     * @param id
     * @throws IOException
     */
    public void deleteMapContainer(Object id) throws IOException;
    
    /**
     * delete a MapContainer from the name container
     * 
     * @param id
     * @param containerName 
     * @throws IOException
     */
    public void deleteMapContainer(Object id,String containerName) throws IOException;

    /**
     * Get a Set of call MapContainer Ids
     * 
     * @return the set of ids
     * @throws IOException
     */
    public Set getMapContainerIds() throws IOException;

    /**
     * Checks if a ListContainer exists in the default container
     * 
     * @param id
     * @return new MapContainer
     * @throws IOException
     */
    public boolean doesListContainerExist(Object id) throws IOException;
    
    /**
     * Checks if a ListContainer exists in the named container
     * 
     * @param id
     * @param containerName 
     * @return new MapContainer
     * @throws IOException
     */
    public boolean doesListContainerExist(Object id,String containerName) throws IOException;

    /**
     * Get a ListContainer with the given id and creates it if it doesn't exist
     * 
     * @param id
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
     public ListContainer getListContainer(Object id) throws IOException;

    /**
     * Get a ListContainer with the given id and creates it if it doesn't exist
     * 
     * @param id
     * @param containerName
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    public ListContainer getListContainer(Object id,String containerName) throws IOException;
    
    /**
     * Get a ListContainer with the given id and creates it if it doesn't exist
     * 
     * @param id
     * @param containerName
     * @param indexType 
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException
     */
    public ListContainer getListContainer(Object id,String containerName,String indexType) throws IOException;

    /**
     * delete a ListContainer from the default container
     * 
     * @param id
     * @throws IOException
     */
    public void deleteListContainer(Object id) throws IOException;
    
    /**
     * delete a ListContainer from the named container
     * 
     * @param id
     * @param containerName 
     * @throws IOException
     */
    public void deleteListContainer(Object id,String containerName) throws IOException;


    /**
     * Get a Set of call ListContainer Ids
     * 
     * @return the set of ids
     * @throws IOException
     */
    public Set getListContainerIds() throws IOException;
    
    /**
     * @return the maxDataFileLength
     */
    public long getMaxDataFileLength();

    /**
     * @param maxDataFileLength the maxDataFileLength to set
     */
    public void setMaxDataFileLength(long maxDataFileLength);
    
    /**
     * @see org.apache.activemq.kaha.IndexTypes
     * @return the default index type
     */
    public String getIndexType();
    
    /**
     * Set the default index type
     * @param type
     * @see org.apache.activemq.kaha.IndexTypes
     */
    public void setIndexType(String type);
}
