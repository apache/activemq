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
     * close the store
     * @throws IOException
     */
    public void close() throws IOException;
    
    
    /**
     * Force all writes to disk
     * @throws IOException
     */
    public void force() throws IOException;
    
    /**
     * empty all the contents of the store
     * @throws IOException
     */
    public void clear() throws IOException;
    
    
    /**
     * delete the store
     * @return true if the delete was successful
     * @throws IOException
     */
    public boolean delete() throws IOException;

    /**
     * Checks if a MapContainer exists
     * @param id
     * @return new MapContainer
     * @throws IOException 
     */
    public boolean doesMapContainerExist(Object id) throws IOException;

    /**
     * Get a MapContainer with the given id - the MapContainer is created if needed
     * @param id
     * @return container for the associated id or null if it doesn't exist
     * @throws IOException 
     */
    public MapContainer getMapContainer(Object id) throws IOException;

    /**
     * delete a container
     * @param id
     * @throws IOException
     */
    public void deleteMapContainer(Object id) throws IOException;

    /**
     * Get a Set of call MapContainer Ids
     * @return the set of ids
     * @throws IOException 
     */
    public Set getMapContainerIds() throws IOException;
    
    /**
     * Checks if a ListContainer exists
     * @param id
     * @return new MapContainer
     * @throws IOException 
     */
    public boolean doesListContainerExist(Object id) throws IOException;

   /**
    * Get a ListContainer with the given id and creates it if it doesn't exist
    * @param id
    * @return container for the associated id or null if it doesn't exist
 * @throws IOException 
    */
   public ListContainer getListContainer(Object id) throws IOException;

   /**
    * delete a ListContainer
    * @param id
    * @throws IOException
    */
   public void deleteListContainer(Object id) throws IOException;

   /**
    * Get a Set of call ListContainer Ids
    * @return the set of ids
 * @throws IOException 
    */
   public Set getListContainerIds() throws IOException;
    
    
}