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

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

import java.io.*;
import java.util.*;

/**
 *
 * @version $Revision$
 */
public abstract class OpenWireCSharpMarshallingScript extends OpenWireJavaMarshallingScript {

    public Object run() {
        filePostFix = ".cs";
        if (destDir == null) {
            destDir = new File("../openwire-dotnet/src/OpenWire.Client/IO");
        }
        
        return super.run();
    }
    

    protected void generateUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        out.print("        ");
        String propertyName = property.getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("info." + propertyName + " = bs.ReadBoolean();");
        }
        else if (type.equals("byte")) {
            out.println("info." + propertyName + " = DataStreamMarshaller.ReadByte(dataIn);");
        }
        else if (type.equals("char")) {
            out.println("info." + propertyName + " = DataStreamMarshaller.ReadChar(dataIn);");
        }
        else if (type.equals("short")) {
            out.println("info." + propertyName + " = DataStreamMarshaller.ReadShort(dataIn);");
        }
        else if (type.equals("int")) {
            out.println("info." + propertyName + " = DataStreamMarshaller.ReadInt(dataIn);");
        }
        else if (type.equals("long")) {
            out.println("info." + propertyName + " = UnmarshalLong(wireFormat, dataIn, bs);");
        }
        else if (type.equals("String")) {
            out.println("info." + propertyName + " = ReadString(dataIn, bs);");
        }
        else if (type.equals("byte[]") || type.equals("ByteSequence")) {
            if (size != null) {
                out.println("info." + propertyName + " = ReadBytes(dataIn, " + size.asInt() + ");");
            }
            else {
                out.println("info." + propertyName + " = ReadBytes(dataIn, bs.ReadBoolean());");
            }
        }
        else if (isThrowable(property.getType())) {
            out.println("info." + propertyName + " = UnmarshalBrokerError(wireFormat, dataIn, bs);");
        }
        else if (isCachedProperty(property)) {
            out.println("info." + propertyName + " = (" + type + ") UnmarshalCachedObject(wireFormat, dataIn, bs);");
        }
        else {
            out.println("info." + propertyName + " = (" + type + ") UnmarshalNestedObject(wireFormat, dataIn, bs);");
        }
    }

    protected void generateUnmarshalBodyForArrayProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        JClass propertyType = property.getType();
        String arrayType = propertyType.getArrayComponentType().getSimpleName();
        String propertyName = property.getSimpleName();
        out.println();
        if (size != null) {
            out.println("        {");
            out.println("            " + arrayType + "[] value = new " + arrayType + "[" + size.asInt() + "];");
            out.println("            " + "for( int i=0; i < " + size.asInt() + "; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") UnmarshalNestedObject(wireFormat,dataIn, bs);");
            out.println("            }");
            out.println("            info." + propertyName + " = value;");
            out.println("        }");
        }
        else {
            out.println("        if (bs.ReadBoolean()) {");
            out.println("            short size = DataStreamMarshaller.ReadShort(dataIn);");
            out.println("            " + arrayType + "[] value = new " + arrayType + "[size];");
            out.println("            for( int i=0; i < size; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") UnmarshalNestedObject(wireFormat,dataIn, bs);");
            out.println("            }");
            out.println("            info." + propertyName + " = value;");
            out.println("        }");
            out.println("        else {");
            out.println("            info." + propertyName + " = null;");
            out.println("        }");
        }
    }

    protected int generateMarshal1Body(PrintWriter out) {
        List properties = getProperties();
        int baseSize = 0;
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty) iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getSimpleName();

            out.print(indent);
            if (type.equals("boolean")) {
                out.println("bs.WriteBoolean(" + getter + ");");
            }
            else if (type.equals("byte")) {
                baseSize += 1;
            }
            else if (type.equals("char")) {
                baseSize += 1;
            }
            else if (type.equals("short")) {
                baseSize += 1;
            }
            else if (type.equals("int")) {
                baseSize += 1;
            }
            else if (type.equals("long")) {
                out.println("rc += Marshal1Long(wireFormat, " + getter + ", bs);");
            }
            else if (type.equals("String")) {
                out.println("rc += WriteString(" + getter + ", bs);");
            }
            else if (type.equals("byte[]") || type.equals("ByteSequence")) {
                if (size == null) {
                    out.println("bs.WriteBoolean(" + getter + "!=null);");
                    out.println("        rc += " + getter + "==null ? 0 : " + getter + ".Length+4;");
                }
                else {
                    baseSize += size.asInt();
                }
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("rc += MarshalObjectArrayConstSize(wireFormat, " + getter + ", bs, " + size.asInt() + ");");
                }
                else {
                    out.println("rc += MarshalObjectArray(wireFormat, " + getter + ", bs);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("rc += MarshalBrokerError(wireFormat, " + getter + ", bs);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("rc += Marshal1CachedObject(wireFormat, " + getter + ", bs);");
                }
                else {
                    out.println("rc += Marshal1NestedObject(wireFormat, " + getter + ", bs);");
                }
            }
        }
        return baseSize;
    }

    protected void generateMarshal2Body(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty) iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getSimpleName();

            out.print(indent);
            if (type.equals("boolean")) {
                out.println("bs.ReadBoolean();");
            }
            else if (type.equals("byte")) {
                out.println("DataStreamMarshaller.WriteByte(" + getter + ", dataOut);");
            }
            else if (type.equals("char")) {
                out.println("DataStreamMarshaller.WriteChar(" + getter + ", dataOut);");
            }
            else if (type.equals("short")) {
                out.println("DataStreamMarshaller.WriteShort(" + getter + ", dataOut);");
            }
            else if (type.equals("int")) {
                out.println("DataStreamMarshaller.WriteInt(" + getter + ", dataOut);");
            }
            else if (type.equals("long")) {
                out.println("Marshal2Long(wireFormat, " + getter + ", dataOut, bs);");
            }
            else if (type.equals("String")) {
                out.println("WriteString(" + getter + ", dataOut, bs);");
            }
            else if (type.equals("byte[]") || type.equals("ByteSequence")) {
                if (size != null) {
                    out.println("dataOut.Write(" + getter + ", 0, " + size.asInt() + ");");
                }
                else {
                    out.println("if(bs.ReadBoolean()) {");
                    out.println("           DataStreamMarshaller.WriteInt(" + getter + ".Length, dataOut);");
                    out.println("           dataOut.Write(" + getter + ");");
                    out.println("        }");
                }
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("MarshalObjectArrayConstSize(wireFormat, " + getter + ", dataOut, bs, " + size.asInt() + ");");
                }
                else {
                    out.println("MarshalObjectArray(wireFormat, " + getter + ", dataOut, bs);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("MarshalBrokerError(wireFormat, " + getter + ", dataOut, bs);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("Marshal2CachedObject(wireFormat, " + getter + ", dataOut, bs);");
                }
                else {
                    out.println("Marshal2NestedObject(wireFormat, " + getter + ", dataOut, bs);");
                }
            }
        }
    }
}
