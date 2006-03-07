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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JProperty;

/**
 * 
 * @version $Revision$
 */
public abstract class OpenWireJavaMarshallingScript extends OpenWireClassesScript {

    protected List concreteClasses = new ArrayList();
    protected File factoryFile;
    protected String factoryFileName = "MarshallerFactory";
    protected String indent = "    ";

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
        String answer = "BaseDataStreamMarshaller";
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

    protected void generateTightUnmarshalBody(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty) iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String propertyTypeName = propertyType.getSimpleName();

            if (propertyType.isArrayType() && !propertyTypeName.equals("byte[]")) {
                generateTightUnmarshalBodyForArrayProperty(out, property, size);
            }
            else {
                generateTightUnmarshalBodyForProperty(out, property, size);
            }
        }
    }

    protected void generateTightUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
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
            out.println("info." + setter + "(tightUnmarshalLong(wireFormat, dataIn, bs));");
        }
        else if (type.equals("String")) {
            out.println("info." + setter + "(tightUnmarshalString(dataIn, bs));");
        }
        else if (type.equals("byte[]")) {
            if (size != null) {
                out.println("info." + setter + "(tightUnmarshalConstByteArray(dataIn, bs, "+ size.asInt() +"));");
            }
            else {
                out.println("info." + setter + "(tightUnmarshalByteArray(dataIn, bs));");
            }
        }
        else if (type.equals("ByteSequence")) {
            out.println("info." + setter + "(tightUnmarshalByteSequence(dataIn, bs));");
        }
        else if (isThrowable(property.getType())) {
            out.println("info." + setter + "((" + property.getType().getQualifiedName() + ") tightUnmarsalThrowable(wireFormat, dataIn, bs));");
        }
        else if (isCachedProperty(property)) {
            out.println("info." + setter + "((" + property.getType().getQualifiedName() + ") tightUnmarsalCachedObject(wireFormat, dataIn, bs));");
        }
        else {
            out.println("info." + setter + "((" + property.getType().getQualifiedName() + ") tightUnmarsalNestedObject(wireFormat, dataIn, bs));");
        }
    }

    protected void generateTightUnmarshalBodyForArrayProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        JClass propertyType = property.getType();
        String arrayType = propertyType.getArrayComponentType().getQualifiedName();
        String setter = property.getSetter().getSimpleName();
        out.println();
        if (size != null) {
            out.println("        {");
            out.println("            " + arrayType + " value[] = new " + arrayType + "[" + size.asInt() + "];");
            out.println("            " + "for( int i=0; i < " + size.asInt() + "; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") tightUnmarsalNestedObject(wireFormat,dataIn, bs);");
            out.println("            }");
            out.println("            info." + setter + "(value);");
            out.println("        }");
        }
        else {
            out.println("        if (bs.readBoolean()) {");
            out.println("            short size = dataIn.readShort();");
            out.println("            " + arrayType + " value[] = new " + arrayType + "[size];");
            out.println("            for( int i=0; i < size; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") tightUnmarsalNestedObject(wireFormat,dataIn, bs);");
            out.println("            }");
            out.println("            info." + setter + "(value);");
            out.println("        }");
            out.println("        else {");
            out.println("            info." + setter + "(null);");
            out.println("        }");
        }
    }

    protected int generateTightMarshal1Body(PrintWriter out) {
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
                baseSize += 2;
            }
            else if (type.equals("short")) {
                baseSize += 2;
            }
            else if (type.equals("int")) {
                baseSize += 4;
            }
            else if (type.equals("long")) {
                out.println("rc+=tightMarshalLong1(wireFormat, " + getter + ", bs);");
            }
            else if (type.equals("String")) {
                out.println("rc += tightMarshalString1(" + getter + ", bs);");
            }
            else if (type.equals("byte[]")) {
                if (size == null) {
                    out.println("rc += tightMarshalByteArray1(" + getter + ", bs);");
                }
                else {
                    out.println("rc += tightMarshalConstByteArray1(" + getter + ", bs, "+size.asInt()+");");
                }
            }
            else if (type.equals("ByteSequence")) {
                out.println("rc += tightMarshalByteSequence1(" + getter + ", bs);");
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("rc += tightMarshalObjectArrayConstSize1(wireFormat, " + getter + ", bs, " + size.asInt() + ");");
                }
                else {
                    out.println("rc += tightMarshalObjectArray1(wireFormat, " + getter + ", bs);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("rc += tightMarshalThrowable1(wireFormat, " + getter + ", bs);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("rc += tightMarshalCachedObject1(wireFormat, (DataStructure)" + getter + ", bs);");
                }
                else {
                    out.println("rc += tightMarshalNestedObject1(wireFormat, (DataStructure)" + getter + ", bs);");
                }
            }
        }
        return baseSize;
    }

    protected void generateTightMarshal2Body(PrintWriter out) {
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
                out.println("tightMarshalLong2(wireFormat, " + getter + ", dataOut, bs);");
            }
            else if (type.equals("String")) {
                out.println("tightMarshalString2(" + getter + ", dataOut, bs);");
            }
            else if (type.equals("byte[]")) {
                if (size != null) {
                    out.println("tightMarshalConstByteArray2(" + getter + ", dataOut, bs, " + size.asInt() + ");");
                }
                else {
                    out.println("tightMarshalByteArray2(" + getter + ", dataOut, bs);");
                }
            }
            else if (type.equals("ByteSequence")) {
                out.println("tightMarshalByteSequence2(" + getter + ", dataOut, bs);");
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("tightMarshalObjectArrayConstSize2(wireFormat, " + getter + ", dataOut, bs, " + size.asInt() + ");");
                }
                else {
                    out.println("tightMarshalObjectArray2(wireFormat, " + getter + ", dataOut, bs);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("tightMarshalThrowable2(wireFormat, " + getter + ", dataOut, bs);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("tightMarshalCachedObject2(wireFormat, (DataStructure)" + getter + ", dataOut, bs);");
                }
                else {
                    out.println("tightMarshalNestedObject2(wireFormat, (DataStructure)" + getter + ", dataOut, bs);");
                }
            }
        }
    }



    protected void generateLooseMarshalBody(PrintWriter out) {
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
                out.println("dataOut.writeBoolean("+ getter + ");");
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
                out.println("looseMarshalLong(wireFormat, " + getter + ", dataOut);");
            }
            else if (type.equals("String")) {
                out.println("looseMarshalString(" + getter + ", dataOut);");
            }
            else if (type.equals("byte[]")) {
                if (size != null) {
                    out.println("looseMarshalConstByteArray(wireFormat, " + getter + ", dataOut, " + size.asInt() + ");");
                }
                else {
                    out.println("looseMarshalByteArray(wireFormat, " + getter + ", dataOut);");
                }
            }
            else if (type.equals("ByteSequence")) {
                out.println("looseMarshalByteSequence(wireFormat, " + getter + ", dataOut);");
            }
            else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("looseMarshalObjectArrayConstSize(wireFormat, " + getter + ", dataOut, " + size.asInt() + ");");
                }
                else {
                    out.println("looseMarshalObjectArray(wireFormat, " + getter + ", dataOut);");
                }
            }
            else if (isThrowable(propertyType)) {
                out.println("looseMarshalThrowable(wireFormat, " + getter + ", dataOut);");
            }
            else {
                if (isCachedProperty(property)) {
                    out.println("looseMarshalCachedObject(wireFormat, (DataStructure)" + getter + ", dataOut);");
                }
                else {
                    out.println("looseMarshalNestedObject(wireFormat, (DataStructure)" + getter + ", dataOut);");
                }
            }
        }
    }


    protected void generateLooseUnmarshalBody(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty) iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String propertyTypeName = propertyType.getSimpleName();

            if (propertyType.isArrayType() && !propertyTypeName.equals("byte[]")) {
                generateLooseUnmarshalBodyForArrayProperty(out, property, size);
            }
            else {
                generateLooseUnmarshalBodyForProperty(out, property, size);
            }
        }
    }

    protected void generateLooseUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        out.print("        ");
        String setter = property.getSetter().getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("info." + setter + "(dataIn.readBoolean());");
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
            out.println("info." + setter + "(looseUnmarshalLong(wireFormat, dataIn));");
        }
        else if (type.equals("String")) {
            out.println("info." + setter + "(looseUnmarshalString(dataIn));");
        }
        else if (type.equals("byte[]")) {
            if (size != null) {
                out.println("info." + setter + "(looseUnmarshalConstByteArray(dataIn, " + size.asInt() + "));");
            }
            else {
                out.println("info." + setter + "(looseUnmarshalByteArray(dataIn));");
            }
        }
        else if (type.equals("ByteSequence")) {
            out.println("info." + setter + "(looseUnmarshalByteSequence(dataIn));");
        }
        else if (isThrowable(property.getType())) {
            out.println("info." + setter + "((" + property.getType().getQualifiedName() + ") looseUnmarsalThrowable(wireFormat, dataIn));");
        }
        else if (isCachedProperty(property)) {
            out.println("info." + setter + "((" + property.getType().getQualifiedName() + ") looseUnmarsalCachedObject(wireFormat, dataIn));");
        }
        else {
            out.println("info." + setter + "((" + property.getType().getQualifiedName() + ") looseUnmarsalNestedObject(wireFormat, dataIn));");
        }
    }

    protected void generateLooseUnmarshalBodyForArrayProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        JClass propertyType = property.getType();
        String arrayType = propertyType.getArrayComponentType().getQualifiedName();
        String setter = property.getSetter().getSimpleName();
        out.println();
        if (size != null) {
            out.println("        {");
            out.println("            " + arrayType + " value[] = new " + arrayType + "[" + size.asInt() + "];");
            out.println("            " + "for( int i=0; i < " + size.asInt() + "; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") looseUnmarsalNestedObject(wireFormat,dataIn);");
            out.println("            }");
            out.println("            info." + setter + "(value);");
            out.println("        }");
        }
        else {
            out.println("        if (dataIn.readBoolean()) {");
            out.println("            short size = dataIn.readShort();");
            out.println("            " + arrayType + " value[] = new " + arrayType + "[size];");
            out.println("            for( int i=0; i < size; i++ ) {");
            out.println("                value[i] = (" + arrayType + ") looseUnmarsalNestedObject(wireFormat,dataIn);");
            out.println("            }");
            out.println("            info." + setter + "(value);");
            out.println("        }");
            out.println("        else {");
            out.println("            info." + setter + "(null);");
            out.println("        }");
        }
    }
}
