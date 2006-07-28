/*
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
package org.apache.activemq.openwire.tool;

import org.codehaus.jam.JClass;

import java.io.File;

/**
 * 
 * @version $Revision$
 */
public abstract class OpenWireCppClassesScript extends OpenWireClassesScript {

    public Object run() {
        filePostFix = getFilePostFix();
        if (destDir == null) {
            destDir = new File("../openwire-cpp/src/command");
        }
        return super.run();
    }

    protected String getFilePostFix() {
        return ".cpp";
    }

    /**
     * Converts the Java type to a C++ type name
     */
    public String toCppType(JClass type) {
        String name = type.getSimpleName();
        if (name.equals("String")) {
            return "p<string>";
        }
        else if (type.isArrayType()) {
		if( name.equals("byte[]") )
                name = "char[]" ;
		else if( name.equals("DataStructure[]") )
                name = "IDataStructure[]" ;

            return "ap<" + name.substring(0, name.length()-2) + ">";
        }
        else if (name.equals("Throwable") || name.equals("Exception")) {
            return "p<BrokerError>";
        }
        else if (name.equals("ByteSequence")) {
            return "char*";
        }
        else if (name.equals("boolean")) {
            return "bool";
        }
        else if (name.equals("long")) {
            return "long long";
        }
        else if (name.equals("byte")) {
            return "char";
        }
        else if( name.equals("Command") || name.equals("DataStructure") ) {
            return "p<I" + name + ">" ;
        }
        else if( !type.isPrimitiveType() ) {
            return "p<" + name + ">" ;
        }
        else {
            return name ;
        }
    }

    /**
     * Converts the Java type to a C++ default value
     */
    public String toCppDefaultValue(JClass type) {
        return "0";
    }
}
