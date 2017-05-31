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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jam.JAnnotation;
import org.codehaus.jam.JAnnotationValue;
import org.codehaus.jam.JClass;
import org.codehaus.jam.JPackage;
import org.codehaus.jam.JProperty;

/**
 * 
 */
public class JavaMarshallingGenerator extends MultiSourceGenerator {

    protected List<JClass> concreteClasses = new ArrayList<JClass>();
    protected File factoryFile;
    protected String factoryFileName = "MarshallerFactory";
    protected String indent = "    ";
    protected String targetDir = "src/main/java";

    public Object run() {
        if (destDir == null) {
            destDir = new File(targetDir + "/org/apache/activemq/openwire/v" + getOpenwireVersion());
        }
        Object answer = super.run();
        processFactory();
        return answer;
    }

    protected void generateFile(PrintWriter out) throws Exception {

        generateLicence(out);
        out.println("");
        out.println("package org.apache.activemq.openwire.v" + getOpenwireVersion() + ";");
        out.println("");
        out.println("import java.io.DataInput;");
        out.println("import java.io.DataOutput;");
        out.println("import java.io.IOException;");
        out.println("");
        out.println("import org.apache.activemq.openwire.*;");
        out.println("import org.apache.activemq.command.*;");
        out.println("");
        out.println("");
        for (int i = 0; i < getJclass().getImportedPackages().length; i++) {
            JPackage pkg = getJclass().getImportedPackages()[i];
            for (int j = 0; j < pkg.getClasses().length; j++) {
                JClass clazz = pkg.getClasses()[j];
                out.println("import " + clazz.getQualifiedName() + ";");
            }
        }

        out.println("");
        out.println("/**");
        out.println(" * Marshalling code for Open Wire Format for " + getClassName() + "");
        out.println(" *");
        out.println(" *");
        out.println(" * NOTE!: This file is auto generated - do not modify!");
        out.println(" *        if you need to make a change, please see the modify the groovy scripts in the");
        out.println(" *        under src/gram/script and then use maven openwire:generate to regenerate ");
        out.println(" *        this file.");
        out.println(" *");
        out.println(" * ");
        out.println(" */");
        out.println("public " + getAbstractClassText() + "class " + getClassName() + " extends " + getBaseClass() + " {");
        out.println("");

        if (!isAbstractClass()) {

            out.println("    /**");
            out.println("     * Return the type of Data Structure we marshal");
            out.println("     * @return short representation of the type data structure");
            out.println("     */");
            out.println("    public byte getDataStructureType() {");
            out.println("        return " + getJclass().getSimpleName() + ".DATA_STRUCTURE_TYPE;");
            out.println("    }");
            out.println("    ");
            out.println("    /**");
            out.println("     * @return a new object instance");
            out.println("     */");
            out.println("    public DataStructure createObject() {");
            out.println("        return new " + getJclass().getSimpleName() + "();");
            out.println("    }");
            out.println("");
        }

        out.println("    /**");
        out.println("     * Un-marshal an object instance from the data input stream");
        out.println("     *");
        out.println("     * @param o the object to un-marshal");
        out.println("     * @param dataIn the data input stream to build the object from");
        out.println("     * @throws IOException");
        out.println("     */");
        out.println("    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn, BooleanStream bs) throws IOException {");
        out.println("        super.tightUnmarshal(wireFormat, o, dataIn, bs);");

        if (!getProperties().isEmpty()) {
            out.println("");
            out.println("        " + getJclass().getSimpleName() + " info = (" + getJclass().getSimpleName() + ")o;");
        }

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.beforeUnmarshall(wireFormat);");
            out.println("        ");
        }

        generateTightUnmarshalBody(out);

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.afterUnmarshall(wireFormat);");
        }

        out.println("");
        out.println("    }");
        out.println("");
        out.println("");
        out.println("    /**");
        out.println("     * Write the booleans that this object uses to a BooleanStream");
        out.println("     */");
        out.println("    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {");

        if (!getProperties().isEmpty()) {
            out.println("");
            out.println("        " + getJclass().getSimpleName() + " info = (" + getJclass().getSimpleName() + ")o;");
        }

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.beforeMarshall(wireFormat);");
        }

        out.println("");
        out.println("        int rc = super.tightMarshal1(wireFormat, o, bs);");
        int baseSize = generateTightMarshal1Body(out);

        out.println("");
        out.println("        return rc + " + baseSize + ";");
        out.println("    }");
        out.println("");
        out.println("    /**");
        out.println("     * Write a object instance to data output stream");
        out.println("     *");
        out.println("     * @param o the instance to be marshaled");
        out.println("     * @param dataOut the output stream");
        out.println("     * @throws IOException thrown if an error occurs");
        out.println("     */");
        out.println("    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutput dataOut, BooleanStream bs) throws IOException {");
        out.println("        super.tightMarshal2(wireFormat, o, dataOut, bs);");
        if (!getProperties().isEmpty()) {
            out.println("");
            out.println("        " + getJclass().getSimpleName() + " info = (" + getJclass().getSimpleName() + ")o;");
        }

        generateTightMarshal2Body(out);

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.afterMarshall(wireFormat);");
        }

        out.println("");
        out.println("    }");
        out.println("");
        out.println("    /**");
        out.println("     * Un-marshal an object instance from the data input stream");
        out.println("     *");
        out.println("     * @param o the object to un-marshal");
        out.println("     * @param dataIn the data input stream to build the object from");
        out.println("     * @throws IOException");
        out.println("     */");
        out.println("    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn) throws IOException {");
        out.println("        super.looseUnmarshal(wireFormat, o, dataIn);");

        if (!getProperties().isEmpty()) {
            out.println("");
            out.println("        " + getJclass().getSimpleName() + " info = (" + getJclass().getSimpleName() + ")o;");
        }

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.beforeUnmarshall(wireFormat);");
            out.println("        ");
        }

        generateLooseUnmarshalBody(out);

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.afterUnmarshall(wireFormat);");
        }

        out.println("");
        out.println("    }");
        out.println("");
        out.println("");
        out.println("    /**");
        out.println("     * Write the booleans that this object uses to a BooleanStream");
        out.println("     */");
        out.println("    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {");

        if (!getProperties().isEmpty()) {
            out.println("");
            out.println("        " + getJclass().getSimpleName() + " info = (" + getJclass().getSimpleName() + ")o;");
        }

        if (isMarshallerAware()) {
            out.println("");
            out.println("        info.beforeMarshall(wireFormat);");
        }

        out.println("");
        out.println("        super.looseMarshal(wireFormat, o, dataOut);");

        generateLooseMarshalBody(out);

        out.println("");
        out.println("    }");
        out.println("}");
    }

    private void generateLicence(PrintWriter out) {
        out.println("/**");
        out.println(" *");
        out.println(" * Licensed to the Apache Software Foundation (ASF) under one or more");
        out.println(" * contributor license agreements.  See the NOTICE file distributed with");
        out.println(" * this work for additional information regarding copyright ownership.");
        out.println(" * The ASF licenses this file to You under the Apache License, Version 2.0");
        out.println(" * (the \"License\"); you may not use this file except in compliance with");
        out.println(" * the License.  You may obtain a copy of the License at");
        out.println(" *");
        out.println(" * http://www.apache.org/licenses/LICENSE-2.0");
        out.println(" *");
        out.println(" * Unless required by applicable law or agreed to in writing, software");
        out.println(" * distributed under the License is distributed on an \"AS IS\" BASIS,");
        out.println(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.");
        out.println(" * See the License for the specific language governing permissions and");
        out.println(" * limitations under the License.");
        out.println(" */");
    }

    protected void processFactory() {
        if (factoryFile == null) {
            factoryFile = new File(destDir, factoryFileName + filePostFix);
        }
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileWriter(factoryFile));
            generateFactory(out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }

    protected void generateFactory(PrintWriter out) {
        generateLicence(out);
        out.println("");
        out.println("package org.apache.activemq.openwire.v" + getOpenwireVersion() + ";");
        out.println("");
        out.println("import org.apache.activemq.openwire.DataStreamMarshaller;");
        out.println("import org.apache.activemq.openwire.OpenWireFormat;");
        out.println("");
        out.println("/**");
        out.println(" * MarshallerFactory for Open Wire Format.");
        out.println(" *");
        out.println(" *");
        out.println(" * NOTE!: This file is auto generated - do not modify!");
        out.println(" *        if you need to make a change, please see the modify the groovy scripts in the");
        out.println(" *        under src/gram/script and then use maven openwire:generate to regenerate ");
        out.println(" *        this file.");
        out.println(" *");
        out.println(" * ");
        out.println(" */");
        out.println("public class MarshallerFactory {");
        out.println("");
        out.println("    /**");
        out.println("     * Creates a Map of command type -> Marshallers");
        out.println("     */");
        out.println("    static final private DataStreamMarshaller marshaller[] = new DataStreamMarshaller[256];");
        out.println("    static {");
        out.println("");

        List<JClass> list = new ArrayList<JClass>(getConcreteClasses());
        Collections.sort(list, new Comparator<JClass>() {
            public int compare(JClass c1, JClass c2) {
                return c1.getSimpleName().compareTo(c2.getSimpleName());
            }
        });

        for (Iterator<JClass> iter = list.iterator(); iter.hasNext();) {
            JClass jclass = iter.next();
            out.println("        add(new " + jclass.getSimpleName() + "Marshaller());");
        }

        out.println("");
        out.println("    }");
        out.println("");
        out.println("    static private void add(DataStreamMarshaller dsm) {");
        out.println("        marshaller[dsm.getDataStructureType()] = dsm;");
        out.println("    }");
        out.println("    ");
        out.println("    static public DataStreamMarshaller[] createMarshallerMap(OpenWireFormat wireFormat) {");
        out.println("        return marshaller;");
        out.println("    }");
        out.println("}");
    }

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
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String propertyTypeName = propertyType.getSimpleName();

            if (propertyType.isArrayType() && !propertyTypeName.equals("byte[]")) {
                generateTightUnmarshalBodyForArrayProperty(out, property, size);
            } else {
                generateTightUnmarshalBodyForProperty(out, property, size);
            }
        }
    }

    protected void generateTightUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        String setter = property.getSetter().getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("        info." + setter + "(bs.readBoolean());");
        } else if (type.equals("byte")) {
            out.println("        info." + setter + "(dataIn.readByte());");
        } else if (type.equals("char")) {
            out.println("        info." + setter + "(dataIn.readChar());");
        } else if (type.equals("short")) {
            out.println("        info." + setter + "(dataIn.readShort());");
        } else if (type.equals("int")) {
            out.println("        info." + setter + "(dataIn.readInt());");
        } else if (type.equals("long")) {
            out.println("        info." + setter + "(tightUnmarshalLong(wireFormat, dataIn, bs));");
        } else if (type.equals("String")) {
            out.println("        info." + setter + "(tightUnmarshalString(dataIn, bs));");
        } else if (type.equals("byte[]")) {
            if (size != null) {
                out.println("        info." + setter + "(tightUnmarshalConstByteArray(dataIn, bs, " + size.asInt() + "));");
            } else {
                out.println("        info." + setter + "(tightUnmarshalByteArray(dataIn, bs));");
            }
        } else if (type.equals("ByteSequence")) {
            out.println("        info." + setter + "(tightUnmarshalByteSequence(dataIn, bs));");
        } else if (isThrowable(property.getType())) {
            out.println("        info." + setter + "((" + property.getType().getQualifiedName() + ") tightUnmarsalThrowable(wireFormat, dataIn, bs));");
        } else if (isCachedProperty(property)) {
            out.println("        info." + setter + "((" + property.getType().getQualifiedName() + ") tightUnmarsalCachedObject(wireFormat, dataIn, bs));");
        } else {
            out.println("        info." + setter + "((" + property.getType().getQualifiedName() + ") tightUnmarsalNestedObject(wireFormat, dataIn, bs));");
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
        } else {
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
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            if (type.equals("boolean")) {
                out.println("        bs.writeBoolean(" + getter + ");");
            } else if (type.equals("byte")) {
                baseSize += 1;
            } else if (type.equals("char")) {
                baseSize += 2;
            } else if (type.equals("short")) {
                baseSize += 2;
            } else if (type.equals("int")) {
                baseSize += 4;
            } else if (type.equals("long")) {
                out.println("        rc+=tightMarshalLong1(wireFormat, " + getter + ", bs);");
            } else if (type.equals("String")) {
                out.println("        rc += tightMarshalString1(" + getter + ", bs);");
            } else if (type.equals("byte[]")) {
                if (size == null) {
                    out.println("        rc += tightMarshalByteArray1(" + getter + ", bs);");
                } else {
                    out.println("        rc += tightMarshalConstByteArray1(" + getter + ", bs, " + size.asInt() + ");");
                }
            } else if (type.equals("ByteSequence")) {
                out.println("        rc += tightMarshalByteSequence1(" + getter + ", bs);");
            } else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("        rc += tightMarshalObjectArrayConstSize1(wireFormat, " + getter + ", bs, " + size.asInt() + ");");
                } else {
                    out.println("        rc += tightMarshalObjectArray1(wireFormat, " + getter + ", bs);");
                }
            } else if (isThrowable(propertyType)) {
                out.println("        rc += tightMarshalThrowable1(wireFormat, " + getter + ", bs);");
            } else {
                if (isCachedProperty(property)) {
                    out.println("        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)" + getter + ", bs);");
                } else {
                    out.println("        rc += tightMarshalNestedObject1(wireFormat, (DataStructure)" + getter + ", bs);");
                }
            }
        }
        return baseSize;
    }

    protected void generateTightMarshal2Body(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            if (type.equals("boolean")) {
                out.println("        bs.readBoolean();");
            } else if (type.equals("byte")) {
                out.println("        dataOut.writeByte(" + getter + ");");
            } else if (type.equals("char")) {
                out.println("        dataOut.writeChar(" + getter + ");");
            } else if (type.equals("short")) {
                out.println("        dataOut.writeShort(" + getter + ");");
            } else if (type.equals("int")) {
                out.println("        dataOut.writeInt(" + getter + ");");
            } else if (type.equals("long")) {
                out.println("        tightMarshalLong2(wireFormat, " + getter + ", dataOut, bs);");
            } else if (type.equals("String")) {
                out.println("        tightMarshalString2(" + getter + ", dataOut, bs);");
            } else if (type.equals("byte[]")) {
                if (size != null) {
                    out.println("        tightMarshalConstByteArray2(" + getter + ", dataOut, bs, " + size.asInt() + ");");
                } else {
                    out.println("        tightMarshalByteArray2(" + getter + ", dataOut, bs);");
                }
            } else if (type.equals("ByteSequence")) {
                out.println("        tightMarshalByteSequence2(" + getter + ", dataOut, bs);");
            } else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("        tightMarshalObjectArrayConstSize2(wireFormat, " + getter + ", dataOut, bs, " + size.asInt() + ");");
                } else {
                    out.println("        tightMarshalObjectArray2(wireFormat, " + getter + ", dataOut, bs);");
                }
            } else if (isThrowable(propertyType)) {
                out.println("        tightMarshalThrowable2(wireFormat, " + getter + ", dataOut, bs);");
            } else {
                if (isCachedProperty(property)) {
                    out.println("        tightMarshalCachedObject2(wireFormat, (DataStructure)" + getter + ", dataOut, bs);");
                } else {
                    out.println("        tightMarshalNestedObject2(wireFormat, (DataStructure)" + getter + ", dataOut, bs);");
                }
            }
        }
    }

    protected void generateLooseMarshalBody(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String type = propertyType.getSimpleName();
            String getter = "info." + property.getGetter().getSimpleName() + "()";

            if (type.equals("boolean")) {
                out.println("        dataOut.writeBoolean(" + getter + ");");
            } else if (type.equals("byte")) {
                out.println("        dataOut.writeByte(" + getter + ");");
            } else if (type.equals("char")) {
                out.println("        dataOut.writeChar(" + getter + ");");
            } else if (type.equals("short")) {
                out.println("        dataOut.writeShort(" + getter + ");");
            } else if (type.equals("int")) {
                out.println("        dataOut.writeInt(" + getter + ");");
            } else if (type.equals("long")) {
                out.println("        looseMarshalLong(wireFormat, " + getter + ", dataOut);");
            } else if (type.equals("String")) {
                out.println("        looseMarshalString(" + getter + ", dataOut);");
            } else if (type.equals("byte[]")) {
                if (size != null) {
                    out.println("        looseMarshalConstByteArray(wireFormat, " + getter + ", dataOut, " + size.asInt() + ");");
                } else {
                    out.println("        looseMarshalByteArray(wireFormat, " + getter + ", dataOut);");
                }
            } else if (type.equals("ByteSequence")) {
                out.println("        looseMarshalByteSequence(wireFormat, " + getter + ", dataOut);");
            } else if (propertyType.isArrayType()) {
                if (size != null) {
                    out.println("        looseMarshalObjectArrayConstSize(wireFormat, " + getter + ", dataOut, " + size.asInt() + ");");
                } else {
                    out.println("        looseMarshalObjectArray(wireFormat, " + getter + ", dataOut);");
                }
            } else if (isThrowable(propertyType)) {
                out.println("        looseMarshalThrowable(wireFormat, " + getter + ", dataOut);");
            } else {
                if (isCachedProperty(property)) {
                    out.println("        looseMarshalCachedObject(wireFormat, (DataStructure)" + getter + ", dataOut);");
                } else {
                    out.println("        looseMarshalNestedObject(wireFormat, (DataStructure)" + getter + ", dataOut);");
                }
            }
        }
    }

    protected void generateLooseUnmarshalBody(PrintWriter out) {
        List properties = getProperties();
        for (Iterator iter = properties.iterator(); iter.hasNext();) {
            JProperty property = (JProperty)iter.next();
            JAnnotation annotation = property.getAnnotation("openwire:property");
            JAnnotationValue size = annotation.getValue("size");
            JClass propertyType = property.getType();
            String propertyTypeName = propertyType.getSimpleName();

            if (propertyType.isArrayType() && !propertyTypeName.equals("byte[]")) {
                generateLooseUnmarshalBodyForArrayProperty(out, property, size);
            } else {
                generateLooseUnmarshalBodyForProperty(out, property, size);
            }
        }
    }

    protected void generateLooseUnmarshalBodyForProperty(PrintWriter out, JProperty property, JAnnotationValue size) {
        String setter = property.getSetter().getSimpleName();
        String type = property.getType().getSimpleName();

        if (type.equals("boolean")) {
            out.println("        info." + setter + "(dataIn.readBoolean());");
        } else if (type.equals("byte")) {
            out.println("        info." + setter + "(dataIn.readByte());");
        } else if (type.equals("char")) {
            out.println("        info." + setter + "(dataIn.readChar());");
        } else if (type.equals("short")) {
            out.println("        info." + setter + "(dataIn.readShort());");
        } else if (type.equals("int")) {
            out.println("        info." + setter + "(dataIn.readInt());");
        } else if (type.equals("long")) {
            out.println("        info." + setter + "(looseUnmarshalLong(wireFormat, dataIn));");
        } else if (type.equals("String")) {
            out.println("        info." + setter + "(looseUnmarshalString(dataIn));");
        } else if (type.equals("byte[]")) {
            if (size != null) {
                out.println("        info." + setter + "(looseUnmarshalConstByteArray(dataIn, " + size.asInt() + "));");
            } else {
                out.println("        info." + setter + "(looseUnmarshalByteArray(dataIn));");
            }
        } else if (type.equals("ByteSequence")) {
            out.println("        info." + setter + "(looseUnmarshalByteSequence(dataIn));");
        } else if (isThrowable(property.getType())) {
            out.println("        info." + setter + "((" + property.getType().getQualifiedName() + ") looseUnmarsalThrowable(wireFormat, dataIn));");
        } else if (isCachedProperty(property)) {
            out.println("        info." + setter + "((" + property.getType().getQualifiedName() + ") looseUnmarsalCachedObject(wireFormat, dataIn));");
        } else {
            out.println("        info." + setter + "((" + property.getType().getQualifiedName() + ") looseUnmarsalNestedObject(wireFormat, dataIn));");
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
        } else {
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

    /**
     * Returns whether or not the given annotation has a mandatory flag on it or
     * not
     */
    protected String getMandatoryFlag(JAnnotation annotation) {
        JAnnotationValue value = annotation.getValue("mandatory");
        if (value != null) {
            String text = value.asString();
            if (text != null && text.equalsIgnoreCase("true")) {
                return "true";
            }
        }
        return "false";
    }

    public List<JClass> getConcreteClasses() {
        return concreteClasses;
    }

    public void setConcreteClasses(List<JClass> concreteClasses) {
        this.concreteClasses = concreteClasses;
    }

    public File getFactoryFile() {
        return factoryFile;
    }

    public void setFactoryFile(File factoryFile) {
        this.factoryFile = factoryFile;
    }

    public String getFactoryFileName() {
        return factoryFileName;
    }

    public void setFactoryFileName(String factoryFileName) {
        this.factoryFileName = factoryFileName;
    }

    public String getIndent() {
        return indent;
    }

    public void setIndent(String indent) {
        this.indent = indent;
    }

    public String getTargetDir() {
        return targetDir;
    }

    public void setTargetDir(String sourceDir) {
        this.targetDir = sourceDir;
    }
}
