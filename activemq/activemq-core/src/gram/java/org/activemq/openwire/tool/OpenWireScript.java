/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
 */
package org.apache.activemq.openwire.tool;

import org.codehaus.gram.GramSupport;
import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JField;
import org.codehaus.jam.JMethod;
import org.codehaus.jam.JProperty;

/**
 * @version $Revision$
 */
public abstract class OpenWireScript extends GramSupport {

    public boolean isValidProperty(JProperty it) {
        JMethod getter = it.getGetter();
        return getter != null && it.getSetter() != null && getter.isStatic() == false
                && getter.getAnnotation("openwire:property") != null;
    }
    
    public boolean isCachedProperty(JProperty it) {
        JMethod getter = it.getGetter();
        if( !isValidProperty(it) )
            return false;
        JAnnotationValue value = getter.getAnnotation("openwire:property").getValue("cache");
        return value!=null && value.asBoolean();
    }

    public boolean isAbstract(JClass j) {
        JField[] fields = j.getFields();
        for (int i = 0; i < fields.length; i++) {
            JField field = fields[i];
            if (field.isStatic() && field.isPublic() && field.isFinal()
                    && field.getSimpleName().equals("DATA_STRUCTURE_TYPE")) {
                return false;
            }
        }
        return true;
    }

    public boolean isThrowable(JClass j) {
        if (j.getQualifiedName().equals(Throwable.class.getName())) {
            return true;
        }
        return j.getSuperclass()!=null && isThrowable(j.getSuperclass());
    }
    
    public boolean isMarshallAware(JClass j) {
        JClass[] interfaces = j.getInterfaces();
        for (int i = 0; i < interfaces.length; i++) {
            if( interfaces[i].getQualifiedName().equals("org.apache.activemq.command.MarshallAware") ) {
                return true;
            }
        }
        return false; //j.getSuperclass()!=null && isMarshallAware(j.getSuperclass());
    }
    
    /**
     * Converts the Java type to a C# type name
     */
    public String toCSharpType(JClass type) {
        String name = type.getSimpleName();
        if (name.equals("String")) {
            return "string";
        }
        else if (name.equals("Throwable") || name.equals("Exception")) {
            return "BrokerError";
        }
        else if (name.equals("ByteSequence")) {
            return "byte[]";
        }
        else if (name.equals("boolean")) {
            return "bool";
        }
        else {
            return name;
        }
    }
    public String getOpenWireOpCode(JClass aClass) {
        return annotationValue(aClass, "openwire:marshaller", "code", "0");
    }
}