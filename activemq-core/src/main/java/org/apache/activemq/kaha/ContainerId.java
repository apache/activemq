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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.activemq.util.IOHelper;

/**
 * Used by RootContainers
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class ContainerId implements Externalizable {
    private static final long serialVersionUID = -8883779541021821943L;
    private Object key;
    private String dataContainerName;

    public ContainerId() {
    }

    public ContainerId(Object key, String dataContainerName) {
        this.key = key;
        this.dataContainerName = dataContainerName;
    }

    /**
     * @return Returns the dataContainerPrefix.
     */
    public String getDataContainerName() {
        return dataContainerName;
    }

    /**
     * @return Returns the key.
     */
    public Object getKey() {
        return key;
    }

    public int hashCode() {
        return key.hashCode() ^ dataContainerName.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != ContainerId.class) {
            return false;
        }
        ContainerId other = (ContainerId)obj;
        return other.key.equals(this.key) && other.dataContainerName.equals(this.dataContainerName);
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(getDataContainerName());
        out.writeObject(key);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dataContainerName = in.readUTF();
        key = in.readObject();
    }

    public String toString() {
        return "CID{" + dataContainerName + ":" + key + "}";
    }
}
