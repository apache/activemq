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
package org.apache.activemq.openwire.tool;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

/**
 * @version $Revision: 381410 $
 */
public class CppMarshallingClassesGenerator extends CppMarshallingHeadersGenerator {

    protected String getFilePostFix() {
        return ".cpp";
    }

    protected void generateUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        out.print("    ");
        String setter = property.getSetter().getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("info." + setter + "( bs.readBoolean() );");
        } else if (type.equals("byte")) {
            out.println("info." + setter + "( DataStreamMarshaller.readByte(dataIn) );");
        } else if (type.equals("char")) {
            out.println("info." + setter + "( DataStreamMarshaller.readChar(dataIn) );");
        } else if (type.equals("short")) {
            out.println("info." + setter + "( DataStreamMarshaller.readShort(dataIn) );");
        } else if (type.equals("int")) {
            out.println("info." + setter + "( DataStreamMarshaller.readInt(dataIn) );");
        } else if (type.equals("long")) {
            out.println("info." + setter + "( UnmarshalLong(wireFormat, dataIn, bs) );");
        } else if (type.equals("String")) {
            out.println("info." + setter + "( readString(dataIn, bs) );");
        } else if (type.equals("byte[]") || type.equals("ByteSequence")) {
            if (size != null) {
                out.println("info." + setter + "( readBytes(dataIn, " + size.asInt() + ") );");
            } else {
                out.println("info." + setter + "( readBytes(dataIn, bs.readBoolean()) );");
            }
        } else if (isThrowable(property.getType())) {
            out.println("info." + setter + "( unmarshalBrokerError(wireFormat, dataIn, bs) );");
        } else if (isCachedProperty(property)) {
            out.println("info." + setter + "( (" + type + ") unmarshalCachedObject(wireFormat, dataIn, bs) );");
        } else {
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
        } else {
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
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            out.print(indent);
            if (type.equals("boolean")) {
                out.println("bs.writeBoolean(" + getter + ");");
            } else if (type.equals("byte")) {
                baseSize += 1;
            } else if (type.equals("char")) {
                baseSize += 1;
            } else if (type.equals("short")) {
                baseSize += 1;
            } else if (type.equals("int")) {
                baseSize += 1;
            } else if (type.equals("long")) {
                out.println("rc += marshal1Long(wireFormat, " + getter + ", bs);");
            } else if (type.equals("String")) {
                out.println("rc += writeString(" + getter + ", bs);");
            } else if (type.equals("byte[]") || type.equals("ByteSequence")) {
                if (size == null) {
                    out.println("bs.writeBoolean(" + getter + "!=null);");
                    out.println("    rc += " + getter + "==null ? 0 : " + getter + ".Length+4;");
                } else {
                    baseSize += size.asInt();
                }
            } else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("rc += marshalObjectArrayConstSize(wireFormat, " + getter + ", bs, " + size.asInt() + ");");
                } else {
                    out.println("rc += marshalObjectArray(wireFormat, " + getter + ", bs);");
                }
            } else if (isThrowable(propertyType)) {
                out.println("rc += marshalBrokerError(wireFormat, " + getter + ", bs);");
            } else {
                if (isCachedProperty(property)) {
                    out.println("rc += marshal1CachedObject(wireFormat, " + getter + ", bs);");
                } else {
                    out.println("rc += marshal1NestedObject(wireFormat, " + getter + ", bs);");
                }
            }
        }
        return baseSize;
    }

    protected void generateMarshal2Body(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            out.print(indent);
            if (type.equals("boolean")) {
                out.println("bs.readBoolean();");
            } else if (type.equals("byte")) {
                out.println("DataStreamMarshaller.writeByte(" + getter + ", dataOut);");
            } else if (type.equals("char")) {
                out.println("DataStreamMarshaller.writeChar(" + getter + ", dataOut);");
            } else if (type.equals("short")) {
                out.println("DataStreamMarshaller.writeShort(" + getter + ", dataOut);");
            } else if (type.equals("int")) {
                out.println("DataStreamMarshaller.writeInt(" + getter + ", dataOut);");
            } else if (type.equals("long")) {
                out.println("marshal2Long(wireFormat, " + getter + ", dataOut, bs);");
            } else if (type.equals("String")) {
                out.println("writeString(" + getter + ", dataOut, bs);");
            } else if (type.equals("byte[]") || type.equals("ByteSequence")) {
                if (size != null) {
                    out.println("dataOut.write(" + getter + ", 0, " + size.asInt() + ");");
                } else {
                    out.println("if(bs.readBoolean()) {");
                    out.println("       DataStreamMarshaller.writeInt(" + getter + ".Length, dataOut);");
                    out.println("       dataOut.write(" + getter + ");");
                    out.println("    }");
                }
            } else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("marshalObjectArrayConstSize(wireFormat, " + getter + ", dataOut, bs, " + size.asInt() + ");");
                } else {
                    out.println("marshalObjectArray(wireFormat, " + getter + ", dataOut, bs);");
                }
            } else if (isThrowable(propertyType)) {
                out.println("marshalBrokerError(wireFormat, " + getter + ", dataOut, bs);");
            } else {
                if (isCachedProperty(property)) {
                    out.println("marshal2CachedObject(wireFormat, " + getter + ", dataOut, bs);");
                } else {
                    out.println("marshal2NestedObject(wireFormat, " + getter + ", dataOut, bs);");
                }
            }
        }
    }

    protected void generateFile(PrintWriter out) throws Exception {
        generateLicence(out);

        out.println("#include \"marshal/" + className + ".hpp\"");
        out.println("");
        out.println("using namespace apache::activemq::client::marshal;");
        out.println("");
        out.println("/*");
        out.println(" *  Marshalling code for Open Wire Format for " + jclass.getSimpleName() + "");
        out.println(" *");
        out.println(" * NOTE!: This file is autogenerated - do not modify!");
        out.println(" *        if you need to make a change, please see the Groovy scripts in the");
        out.println(" *        activemq-core module");
        out.println(" */");
        out.println("");
        out.println("" + className + "::" + className + "()");
        out.println("{");
        out.println("    // no-op");
        out.println("}");
        out.println("");
        out.println("" + className + "::~" + className + "()");
        out.println("{");
        out.println("    // no-op");
        out.println("}");
        out.println("");

        if (!isAbstractClass()) {
            out.println("");
            out.println("");
            out.println("IDataStructure* " + className + "::createObject() ");
            out.println("{");
            out.println("    return new " + jclass.getSimpleName() + "();");
            out.println("}");
            out.println("");
            out.println("char " + className + "::getDataStructureType() ");
            out.println("{");
            out.println("    return " + jclass.getSimpleName() + ".ID_" + jclass.getSimpleName() + ";");
            out.println("}");
        }

        out.println("");
        out.println("    /* ");
        out.println("     * Un-marshal an object instance from the data input stream");
        out.println("     */ ");
        out.println("void " + className + "::unmarshal(ProtocolFormat& wireFormat, Object o, BinaryReader& dataIn, BooleanStream& bs) ");
        out.println("{");
        out.println("    base.unmarshal(wireFormat, o, dataIn, bs);");

        List properties = getProperties();
        boolean marshallerAware = isMarshallerAware();
        if (!properties.isEmpty() || marshallerAware) {
            out.println("");
            out.println("    " + jclass.getSimpleName() + "& info = (" + jclass.getSimpleName() + "&) o;");
        }

        if (marshallerAware) {
            out.println("");
            out.println("    info.beforeUnmarshall(wireFormat);");
            out.println("        ");
        }

        generateTightUnmarshalBody(out);

        if (marshallerAware) {
            out.println("");
            out.println("    info.afterUnmarshall(wireFormat);");
        }

        out.println("");
        out.println("}");
        out.println("");
        out.println("");
        out.println("/*");
        out.println(" * Write the booleans that this object uses to a BooleanStream");
        out.println(" */");
        out.println("int " + className + "::marshal1(ProtocolFormat& wireFormat, Object& o, BooleanStream& bs) {");
        out.println("    " + jclass.getSimpleName() + "& info = (" + jclass.getSimpleName() + "&) o;");

        if (marshallerAware) {
            out.println("");
            out.println("    info.beforeMarshall(wireFormat);");
        }

        out.println("");
        out.println("    int rc = base.marshal1(wireFormat, info, bs);");

        int baseSize = generateMarshal1Body(out);

        out.println("");
        out.println("    return rc + " + baseSize + ";");
        out.println("}");
        out.println("");
        out.println("/* ");
        out.println(" * Write a object instance to data output stream");
        out.println(" */");
        out.println("void " + className + "::marshal2(ProtocolFormat& wireFormat, Object& o, BinaryWriter& dataOut, BooleanStream& bs) {");
        out.println("    base.marshal2(wireFormat, o, dataOut, bs);");

        if (!properties.isEmpty() || marshallerAware) {
            out.println("");
            out.println("    " + jclass.getSimpleName() + "& info = (" + jclass.getSimpleName() + "&) o;");
        }

        generateMarshal2Body(out);

        if (marshallerAware) {
            out.println("");
            out.println("    info.afterMarshall(wireFormat);");
        }

        out.println("");
        out.println("}");
    }

    @SuppressWarnings("unchecked")
    public void generateFactory(PrintWriter out) {
        generateLicence(out);
        out.println("");
        out.println("// Marshalling code for Open Wire Format");
        out.println("//");
        out.println("//");
        out.println("// NOTE!: This file is autogenerated - do not modify!");
        out.println("//        if you need to make a change, please see the Groovy scripts in the");
        out.println("//        activemq-openwire module");
        out.println("//");
        out.println("");
        out.println("#include \"marshal/" + className + ".hpp\"");
        out.println("");

        List list = new ArrayList(getConcreteClasses());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                JClass c1 = (JClass)o1;
                JClass c2 = (JClass)o2;
                return c1.getSimpleName().compareTo(c2.getSimpleName());
            }
        });

        for (Iterator iter = list.iterator(); iter.hasNext();) {
            JClass jclass = (JClass)iter.next();
            out.println("#include \"marshal/" + jclass.getSimpleName() + "Marshaller.hpp\"");
        }

        out.println("");
        out.println("");
        out.println("using namespace apache::activemq::client::marshal;");
        out.println("");
        out.println("");
        out.println("void MarshallerFactory::configure(ProtocolFormat& format) ");
        out.println("{");

        for (Iterator iter = list.iterator(); iter.hasNext();) {
            JClass jclass = (JClass)iter.next();
            out.println("    format.addMarshaller(new " + jclass.getSimpleName() + "Marshaller());");
        }

        out.println("");
        out.println("}");

    }
}
