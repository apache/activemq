/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.kaha.impl;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.Store;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
/**
 * Implementation of a Store
 * 
 * @version $Revision: 1.2 $
 */
public class StoreImpl implements Store{
    private static final Log log = LogFactory.getLog(StoreImpl.class);

    private final Object mutex=new Object();
    private RandomAccessFile dataFile;
    private Map mapContainers=new ConcurrentHashMap();
    private Map listContainers=new ConcurrentHashMap();
    private RootContainer rootMapContainer;
    private RootContainer rootListContainer;
    private String name;
    private StoreReader reader;
    private StoreWriter writer;
    private FreeSpaceManager freeSpaceManager;
    protected boolean closed=false;
    protected Thread shutdownHook;

    public StoreImpl(String name,String mode) throws IOException{
        this.name=name;
        this.dataFile=new RandomAccessFile(name,mode);
        this.reader = new StoreReader(this.dataFile);
        this.writer = new StoreWriter(this.dataFile);
        File file = new File(name);
        log.info("Kaha Store opened " + file.getAbsolutePath());
        freeSpaceManager=new FreeSpaceManager(this.writer,this.reader);
        initialization();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#close()
     */
    public void close() {
        synchronized(mutex){
            if(!closed){
                try {
                for(Iterator i=mapContainers.values().iterator();i.hasNext();){
                    MapContainerImpl container=(MapContainerImpl) i.next();
                    container.close();
                }
                for(Iterator i=listContainers.values().iterator();i.hasNext();){
                    ListContainerImpl container=(ListContainerImpl) i.next();
                    container.close();
                }
                force();
                dataFile.close();
                closed=true;
                }catch(IOException e){
                    log.debug("Failed to close the store cleanly",e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#force()
     */
    public void force() throws IOException{
        checkClosed();
        synchronized(mutex){
            dataFile.getFD().sync();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#clear()
     */
    public void clear(){
        checkClosed();
        for(Iterator i=mapContainers.values().iterator();i.hasNext();){
            MapContainer container=(MapContainer) i.next();
            container.clear();
        }
        for(Iterator i=listContainers.values().iterator();i.hasNext();){
            ListContainer container=(ListContainer) i.next();
            container.clear();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#delete()
     */
    public boolean delete() throws IOException{
        checkClosed();
        dataFile.close();
        File file=new File(name);
        return file.delete();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#doesMapContainerExist(java.lang.Object)
     */
    public boolean doesMapContainerExist(Object id){
        return mapContainers.containsKey(id);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#getContainer(java.lang.Object)
     */
    public MapContainer getMapContainer(Object id) throws IOException{
        checkClosed();
        synchronized(mutex){
            MapContainer result=(MapContainerImpl) mapContainers.get(id);
            if(result==null){
                LocatableItem root=new LocatableItem();
                rootMapContainer.addRoot(id,root);
                result=new MapContainerImpl(id,this,root);
                mapContainers.put(id,result);
            }
            return result;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#deleteContainer(java.lang.Object)
     */
    public void deleteMapContainer(Object id) throws IOException{
        checkClosed();
        synchronized(mutex){
            if(doesMapContainerExist(id)){
                MapContainer container=getMapContainer(id);
                if(container!=null){
                    container.load();
                    container.clear();
                    rootMapContainer.remove(id);
                    mapContainers.remove(id);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#getContainerKeys()
     */
    public Set getMapContainerIds(){
        checkClosed();
        return java.util.Collections.unmodifiableSet(mapContainers.keySet());
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#doesListContainerExist(java.lang.Object)
     */
    public boolean doesListContainerExist(Object id){
        return listContainers.containsKey(id);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#getListContainer(java.lang.Object)
     */
    public ListContainer getListContainer(Object id) throws IOException{
        checkClosed();
        synchronized(mutex){
            ListContainer result=(ListContainerImpl) listContainers.get(id);
            if(result==null){
                LocatableItem root=new LocatableItem();
                rootListContainer.addRoot(id,root);
                result=new ListContainerImpl(id,this,root);
                listContainers.put(id,result);
            }
            return result;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#deleteListContainer(java.lang.Object)
     */
    public void deleteListContainer(Object id) throws IOException{
        checkClosed();
        synchronized(mutex){
            if(doesListContainerExist(id)){
                ListContainer container=getListContainer(id);
                if(container!=null){
                    container.load();
                    container.clear();
                    rootListContainer.remove(id);
                    listContainers.remove(id);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.Store#getListContainerIds()
     */
    public Set getListContainerIds(){
        checkClosed();
        return java.util.Collections.unmodifiableSet(listContainers.keySet());
    }

    public void dumpFreeSpace(PrintWriter printer){
        checkClosed();
        synchronized(mutex){
            freeSpaceManager.dump(printer);
        }
    }


    protected long storeItem(Marshaller marshaller,Object payload,Item item) throws IOException{
        synchronized(mutex){
            int payloadSize = writer.loadPayload(marshaller, payload, item);
            item.setSize(payloadSize);
            // free space manager will set offset and write any headers required
            // so the position should now be correct for writing
            item=freeSpaceManager.getFreeSpace(item);
            writer.storeItem(item,payloadSize);
        }
        return item.getOffset();
    }

    protected Object readItem(Marshaller marshaller,Item item) throws IOException{
        synchronized(mutex){
            return reader.readItem(marshaller, item);
        }
    }
    
    protected void readHeader(Item item) throws IOException{
        synchronized(mutex){
            reader.readHeader(item);
        }
    }
    
    protected void readLocation(Item item) throws IOException{
        synchronized(mutex){
            reader.readLocation(item);
        }
    }

    protected void updateItem(Item item) throws IOException{
        synchronized(mutex){
            writer.updatePayload(item);
        }
    }

    protected void removeItem(Item item) throws IOException{
        synchronized(mutex){
            freeSpaceManager.addFreeSpace(item);
        }
    }

    private void initialization() throws IOException{
        //add shutdown hook
       addShutdownHook();
        // check for new file
        LocatableItem mapRoot=new LocatableItem();
        LocatableItem listRoot=new LocatableItem();
        if(dataFile.length()==0){
            writer.allocateSpace(FreeSpaceManager.RESIZE_INCREMENT);
            storeItem(RootContainer.rootMarshaller,"mapRoot",mapRoot);
            storeItem(RootContainer.rootMarshaller,"listRoot",listRoot);
        }else{
            freeSpaceManager.scanStoredItems();
            dataFile.seek(FreeSpaceManager.ROOT_SIZE);
            mapRoot.setOffset(FreeSpaceManager.ROOT_SIZE);
            readItem(RootContainer.rootMarshaller,mapRoot);
            listRoot.setOffset(dataFile.getFilePointer());
            readItem(RootContainer.rootMarshaller,listRoot);
        }
        rootMapContainer=new RootContainer("root",this,mapRoot);
        rootMapContainer.load();
        Set keys=rootMapContainer.keySet();
        for(Iterator i=keys.iterator();i.hasNext();){
            Object id=i.next();
            if(id!=null){
                LocatableItem item=(LocatableItem) rootMapContainer.get(id);
                if(item!=null){
                    MapContainer container=new MapContainerImpl(id,this,item);
                    mapContainers.put(id,container);
                }
            }
        }
        rootListContainer=new RootContainer("root",this,listRoot);
        rootListContainer.load();
        keys=rootListContainer.keySet();
        for(Iterator i=keys.iterator();i.hasNext();){
            Object id=i.next();
            if(id!=null){
                LocatableItem item=(LocatableItem) rootListContainer.get(id);
                if(item!=null){
                    ListContainer container=new ListContainerImpl(id,this,item);
                    listContainers.put(id,container);
                }
            }
        }
    }
    
    

    

    protected void checkClosed(){
        if(closed){
            throw new RuntimeStoreException("The store is closed");
        }
    }
    
    
    protected void addShutdownHook() {
      
            shutdownHook = new Thread("Kaha Store implementation is shutting down") {
                public void run() {
                    if (!closed){
                        try{
                            //this needs to be really quick so ...
                            closed = true;
                            dataFile.close();
                        }catch(Throwable e){
                            log.error("Failed to close data file",e);
                        }
                    }
                }
            };
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        
    }

    protected void removeShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            }
            catch (Exception e) {
                log.warn("Failed to run shutdown hook",e);
            }
        }
    }
    
}
