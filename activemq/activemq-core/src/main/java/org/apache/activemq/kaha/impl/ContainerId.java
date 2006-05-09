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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
/**
 * Used by RootContainers
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ContainerId implements Externalizable{
    private static final long serialVersionUID=-8883779541021821943L;
    private Object key;
    private String dataContainerPrefix;

    /**
     * @return Returns the dataContainerPrefix.
     */
    public String getDataContainerPrefix(){
        return dataContainerPrefix;
    }

    /**
     * @param dataContainerPrefix The dataContainerPrefix to set.
     */
    public void setDataContainerPrefix(String dataContainerPrefix){
        this.dataContainerPrefix=dataContainerPrefix;
    }

    /**
     * @return Returns the key.
     */
    public Object getKey(){
        return key;
    }

    /**
     * @param key The key to set.
     */
    public void setKey(Object key){
        this.key=key;
    }
    
    public int hashCode(){
        return key.hashCode();
    }
    
    public boolean equals(Object obj){
        boolean result = false;
        if (obj != null && obj instanceof ContainerId){
            ContainerId other = (ContainerId) obj;
            result = other.key.equals(this.key);
        }
        return result;
    }

    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeUTF(getDataContainerPrefix());
        out.writeObject(key);
    }

    public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException{
        dataContainerPrefix=in.readUTF();
        key=in.readObject();
    }
}