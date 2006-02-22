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

import org.codehaus.jam.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 
 * @version $Revision$
 */
public abstract class OpenWireJavaMarshallingScript extends OpenWireClassesScript {

    protected List concreteClasses = new ArrayList();
    protected File factoryFile;
    protected String factoryFileName = "MarshallerFactory";
    protected String indent = "        ";

    public Object run() {
        if (destDir == null) {
            destDir = new File("src/main/java/org/apache/activemq/openwire/v" + getOpenwireVersion());
        }
        Object answer = super.run();
        processFactory();
        return answer;
    }

    protected void processFactory() {
        if (factoryFile == null) {
            factoryFile = new File(destDir, factoryFileName + filePostFix);
        }
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(factoryFile));
            generateFactory(out);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            if (out != null) {
                out.close();
            }
        }
    }

    protected abstract void generateFactory(PrintWriter out);

    protected void processClass(JClass jclass) {
        super.processClass(jclass);

        if (!jclass.isAbstract()) {
            concreteClasses.add(jclass);
        }
    }

    protected String getClassName(JClass jclass) {
        return super.getClassName(jclass) + "Marshaller";
    }

    protected String getBaseClassName(JClass jclass) {
        String answer = "DataStreamMarshaller";
        JClass superclass = jclass.getSuperclass();
        if (superclass != null) {
            String superName = superclass.getSimpleName();
            if (!superName.equals("Object") && !superName.equals("JNDIBaseStorable") && !superName.equals("DataStructureSupport")) {
                answer = superName + "Marshaller";
            }
        }
        return answer;
    }

    protected void initialiseManuallyMaintainedClasses() {
    }

    protected void generateUnmarshalBody(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty) iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String propertyTypeName = propertyType.getSimpleName();

            if (propertyType.isArrayType() && !propertyTypeName.equals("byte[]")) {
                generateUnmarshalBodyForArrayProperty(out, property, size);
            }
            else {
                generateUnmarshalBodyForProperty(out, property, size);
            }
        }
    }

    protected void generateUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        out.print("        ");
        String setter = property.getSetter().getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("info." + setter + "(bs.readBoolean());");
        }
        else if (type.equals("byte")) {
            out.println("info." + setter + "(dataIn.readByte());");
        }
        else if (type.equals("char")) {
            out.println("info." + setter + "(dataIn.readChar());");
        }
        else if (type.equals("short")) {
            out.println("info." + setter + "(dataIn.readShort());");
        }
        else if (type.equals("int")) {
            out.println("info." + setter + "(dataIn.readInt());");
        }
        else if (type.equals("long")) {
            out.println("info." + setter + "(unmarshalLong(wireFormat, dataIn, bs));");
        }
        else if (type.equals("String")) {
            out.println("info." + setter + "(readString(dataIn, bs));");
        }
        else if (type.equals("byte[]")) {
            if (size != null) {
                out.println("{");
                out.println("            byte data[] = new byte[" + size.asInt() + "];");
                out.println("            dataIn.readFully(data);");
                out.println("            info." + setter + "(data);");
                out.println("        }");
            }
            else {
                out.println("if( bs.readBoolean() ) {");
                out.println("            int size = dataIn.readInt();");
                out.println("            byte data[] = new byte[size];");
                out.println("            dataIn.readFully(data);");
                out.println("            info." + setter + "(data);");
                out.println("            } else {");
                out.println("            info." + setter + "(null);");
                out.println("        }");
            }
        }
        else if (type.equals("ByteSequence")) {
            out.println("if( bs.readBoolean() ) {");
            out.println("            int size = dataIn.readInt();");
            out.println("            byte data[] = new byte[size];");
            out.println("            dataIn.readFully(data);");
            out.println("            info." + setter + "(new org.activeio.ByteSequence(data,0,size));");
            out.println("            } else {");
            out.println("            info." + setter + "(null);");
            out.println("        }");
        }
        else if (isThrowable(property.getType())) {
            out.println("info." + setter + "((" + type + ") unmarsalThrowable(wireFormat, dataIn, bs));");
        }
        else if (isCachedProperty(property)) {
            out.println("info." + setter + "((" + type + ") unmarsalCachedObject(wireFormat, dataIn, bs));");
        }
        else {
            out.println("info." + setter + "((" + type + ") unmarsalNestedObject(wireFormat, dataIn, bs));");
        }
    }

    protected void generateUnmarshalBodyForArrayProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        JClass propertyType = property.getType();
        String arrayType = propertyType.getArrayComponentType().getSimpleName();
        String setter = property.getSetter().getSimpleName();
        out.println();
        if (size != null) {
            out.println("        {");
            out.println("            " + arrayType + " value[] = new " + arrayType + "[" + size.asInt() + "];");
            out.println("            " + "for( int i=0; i < " + size.asInt() + "; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") unmarsalNestedObject(wireFormat,dataIn, bs);");
            out.println("            }");
            out.println("            info." + setter + "(value);");
            out.println("        }");
        }
        else {
            out.println("        if (bs.readBoolean()) {");
            out.println("            short size = dataIn.readShort();");
            out.println("            " + arrayType + " value[] = new " + arrayType + "[size];");
            out.println("            for( int i=0; i < size; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") unmarsalNestedObject(wireFormat,dataIn, bs);");
            out.println("            }");
            out.println("            info." + setter + "(value);");
            out.println("        }");
            out.println("        else {");
            out.println("            info." + setter + "(null);");
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
                out.println("rc+=marshal1Long(wireFormat, " + getter + ", bs);");
            }
            else if (type.equals("String")) {
                out.println("rc += writeString(" + getter + ", bs);");
            }
            else if (type.equals("byte[]")) {
                if (size == null) {
                    out.println("bs.writeBoolean(" + getter + "!=null);");
                    out.println("        rc += " + getter + "==null ? 0 : " + getter + ".length+4;");
                }
                else {
                    baseSize += size.asInt();
                }
            }
            else if (type.equals("ByteSequence")) {
                out.println("bs.writeBoolean(" + getter + "!=null);");
                out.println("        rc += " + getter + "==null ? 0 : " + getter + ".getLength()+4;");
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
                out.println("rc += marshalThrowable(wireFormat, " + getter + ", bs);");
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
                out.println("dataOut.writeByte(" + getter + ");");
            }
            else if (type.equals("char")) {
                out.println("dataOut.writeChar(" + getter + ");");
            }
            else if (type.equals("short")) {
                out.println("dataOut.writeShort(" + getter + ");");
            }
            else if (type.equals("int")) {
                out.println("dataOut.writeInt(" + getter + ");");
            }
            else if (type.equals("long")) {
                out.println("marshal2Long(wireFormat, " + getter + ", dataOut, bs);");
            }
            else if (type.equals("String")) {
                out.println("writeString(" + getter + ", dataOut, bs);");
            }
            else if (type.equals("byte[]")) {
                if (size != null) {
                    out.println("dataOut.write(" + getter + ", 0, " + size.asInt() + ");");
                }
                else {
                    out.println("if(bs.readBoolean()) {");
                    out.println("           dataOut.writeInt(" + getter + ".length);");
                    out.println("           dataOut.write(" + getter + ");");
                    out.println("        }");
                }
            }
            else if (type.equals("ByteSequence")) {
                out.println("if(bs.readBoolean()) {");
                out.println("           org.activeio.ByteSequence data = " + getter + ";");
                out.println("           dataOut.writeInt(data.getLength());");
                out.println("           dataOut.write(data.getData(), data.getOffset(), data.getLength());");
                out.println("        }");
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
                out.println("marshalThrowable(wireFormat, " + getter + ", dataOut, bs);");
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
