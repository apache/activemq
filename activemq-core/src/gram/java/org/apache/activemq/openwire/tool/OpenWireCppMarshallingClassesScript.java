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
package org.apache.activemq.openwire.tool;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

import java.io.PrintWriter;
import java.util.Iterator;
import java.util.List;

/**
 *
 * @version $Revision$
 */
public abstract class OpenWireCppMarshallingClassesScript extends OpenWireCppMarshallingHeadersScript {

    protected String getFilePostFix() {
        return ".cpp";
    }

    protected void generateUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        out.print("    ");
        String setter = property.getSetter().getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("info." + setter + "( bs.readBoolean() );");
        }
        else if (type.equals("byte")) {
            out.println("info." + setter + "( DataStreamMarshaller.readByte(dataIn) );");
        }
        else if (type.equals("char")) {
            out.println("info." + setter + "( DataStreamMarshaller.readChar(dataIn) );");
        }
        else if (type.equals("short")) {
            out.println("info." + setter + "( DataStreamMarshaller.readShort(dataIn) );");
        }
        else if (type.equals("int")) {
            out.println("info." + setter + "( DataStreamMarshaller.readInt(dataIn) );");
        }
        else if (type.equals("long")) {
            out.println("info." + setter + "( UnmarshalLong(wireFormat, dataIn, bs) );");
        }
        else if (type.equals("String")) {
            out.println("info." + setter + "( readString(dataIn, bs) );");
        }
        else if (type.equals("byte[]") || type.equals("ByteSequence")) {
            if (size != null) {
                out.println("info." + setter + "( readBytes(dataIn, " + size.asInt() + ") );");
            }
            else {
                out.println("info." + setter + "( readBytes(dataIn, bs.readBoolean()) );");
            }
        }
        else if (isThrowable(property.getType())) {
            out.println("info." + setter + "( unmarshalBrokerError(wireFormat, dataIn, bs) );");
        }
        else if (isCachedProperty(property)) {
            out.println("info." + setter + "( (" + type + ") unmarshalCachedObject(wireFormat, dataIn, bs) );");
        }
        else {
            out.println("info." + setter + "( (" + type + ") unmarshalNestedObject(wireFormat, dataIn, bs) );");
        }
    }

    protected void generateUnmarshalBodyForArrayProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        JClass propertyType = property.getType();
        String arrayType = propertyType.getArrayComponentType().getSimpleName();
        String setter = property.getGetter().getSimpleName();
        out.println();
        if (size != null) {
            out.println("    {");
            out.println("        " + arrayType + "[] value = new " + arrayType + "[" + size.asInt() + "];");
            out.println("        " + "for( int i=0; i < " + size.asInt() + "; i++ ) {");
            out.println("            value[i] = (" + arrayType + ") unmarshalNestedObject(wireFormat,dataIn, bs);");
            out.println("        }");
            out.println("        info." + setter + "( value );");
            out.println("    }");
        }
        else {
            out.println("    if (bs.readBoolean()) {");
            out.println("        short size = DataStreamMarshaller.readShort(dataIn);");
            out.println("        " + arrayType + "[] value = new " + arrayType + "[size];");
            out.println("        for( int i=0; i < size; i++ ) {");
            out.println("            value[i] = (" + arrayType + ") unmarshalNestedObject(wireFormat,dataIn, bs);");
            out.println("        }");
            out.println("        info." + setter + "( value );");
            out.println("    }");
            out.println("    else {");
            out.println("        info." + setter + "( null );");
            out.println("    }");
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
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            out.print(indent);
            if (type.equals("boolean")) {
                out.println("bs.writeBoolean(" + getter + ");");
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
                out.println("rc += marshal1Long(wireFormat, " + getter + ", bs);");
            }
            else if (type.equals("String")) {
                out.println("rc += writeString(" + getter + ", bs);");
            }
            else if (type.equals("byte[]") || type.equals("ByteSequence")) {
                if (size == null) {
                    out.println("bs.writeBoolean(" + getter + "!=null);");
                    out.println("    rc += " + getter + "==null ? 0 : " + getter + ".Length+4;");
                }
                else {
                    baseSize += size.asInt();
                }
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("rc += marshalObjectArrayConstSize(wireFormat, " + getter + ", bs, " + size.asInt() + ");");
                }
                else {
                    out.println("rc += marshalObjectArray(wireFormat, " + getter + ", bs);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("rc += marshalBrokerError(wireFormat, " + getter + ", bs);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("rc += marshal1CachedObject(wireFormat, " + getter + ", bs);");
                }
                else {
                    out.println("rc += marshal1NestedObject(wireFormat, " + getter + ", bs);");
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
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            out.print(indent);
            if (type.equals("boolean")) {
                out.println("bs.readBoolean();");
            }
            else if (type.equals("byte")) {
                out.println("DataStreamMarshaller.writeByte(" + getter + ", dataOut);");
            }
            else if (type.equals("char")) {
                out.println("DataStreamMarshaller.writeChar(" + getter + ", dataOut);");
            }
            else if (type.equals("short")) {
                out.println("DataStreamMarshaller.writeShort(" + getter + ", dataOut);");
            }
            else if (type.equals("int")) {
                out.println("DataStreamMarshaller.writeInt(" + getter + ", dataOut);");
            }
            else if (type.equals("long")) {
                out.println("marshal2Long(wireFormat, " + getter + ", dataOut, bs);");
            }
            else if (type.equals("String")) {
                out.println("writeString(" + getter + ", dataOut, bs);");
            }
            else if (type.equals("byte[]") || type.equals("ByteSequence")) {
                if (size != null) {
                    out.println("dataOut.write(" + getter + ", 0, " + size.asInt() + ");");
                }
                else {
                    out.println("if(bs.readBoolean()) {");
                    out.println("       DataStreamMarshaller.writeInt(" + getter + ".Length, dataOut);");
                    out.println("       dataOut.write(" + getter + ");");
                    out.println("    }");
                }
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("marshalObjectArrayConstSize(wireFormat, " + getter + ", dataOut, bs, " + size.asInt() + ");");
                }
                else {
                    out.println("marshalObjectArray(wireFormat, " + getter + ", dataOut, bs);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("marshalBrokerError(wireFormat, " + getter + ", dataOut, bs);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("marshal2CachedObject(wireFormat, " + getter + ", dataOut, bs);");
                }
                else {
                    out.println("marshal2NestedObject(wireFormat, " + getter + ", dataOut, bs);");
                }
            }
        }
    }
}
