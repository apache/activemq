/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
**/
package org.activemq.command;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.LinkedHashMap;

/**
 * 
 * @openwire:marshaller
 * @version $Revision: 1.11 $
 */
abstract public class BaseCommand implements Command {

    protected short commandId;
    protected boolean responseRequired;
    
    public void copy(BaseCommand copy) {
        copy.commandId = commandId;
        copy.responseRequired = responseRequired;
    }    

    public boolean isWireFormatInfo() {
        return false;
    }

    public boolean isBrokerInfo() {
        return false;
    }

    public boolean isResponse() {
        return false;
    }
    
    public boolean isMessageDispatch() {
        return false;
    }
    
    public boolean isMessage() {
        return false;
    }
    
    public boolean isMarshallAware() {
        return false;
    }
    
    public boolean isMessageAck() {
        return false;
    }

    /**
     * @openwire:property version=1
     */
    public short getCommandId() {
        return commandId;
    }

    public void setCommandId(short commandId) {
        this.commandId = commandId;
    }

    /**
     * @openwire:property version=1
     */
    public boolean isResponseRequired() {
        return responseRequired;
    }

    public void setResponseRequired(boolean responseRequired) {
        this.responseRequired = responseRequired;
    }

    public String toString() {
        LinkedHashMap map = new LinkedHashMap();
        addFields(map, getClass());
        return simpleName(getClass())+" "+map;
    }

    public static String simpleName(Class clazz) {
        String name = clazz.getName();
        int p = name.lastIndexOf(".");
        if( p >= 0 ) {
            name = name.substring(p+1);
        }
        return name;
    }
    

    private void addFields(LinkedHashMap map, Class clazz) {
        
        if( clazz!=BaseCommand.class ) 
            addFields( map, clazz.getSuperclass() );
        
        Field[] fields = clazz.getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if( Modifier.isStatic(field.getModifiers()) || 
                Modifier.isTransient(field.getModifiers()) ||
                Modifier.isPrivate(field.getModifiers())  ) {
                continue;
            }
            
            try {
                map.put(field.getName(), field.get(this));
            } catch (Throwable e) {
                e.printStackTrace();
            }
        }
        
    }


}
