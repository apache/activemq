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
package org.apache.activemq.protobuf.compiler;

import static org.apache.activemq.protobuf.WireFormat.WIRETYPE_FIXED32;
import static org.apache.activemq.protobuf.WireFormat.WIRETYPE_FIXED64;
import static org.apache.activemq.protobuf.WireFormat.WIRETYPE_LENGTH_DELIMITED;
import static org.apache.activemq.protobuf.WireFormat.WIRETYPE_VARINT;
import static org.apache.activemq.protobuf.WireFormat.makeTag;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.activemq.protobuf.compiler.parser.ParseException;
import org.apache.activemq.protobuf.compiler.parser.ProtoParser;

public class AltJavaGenerator {

    private File out = new File(".");
    private File[] path = new File[]{new File(".")};

    private ProtoDescriptor proto;
    private String javaPackage;
    private String outerClassName;
    private PrintWriter w;
    private int indent;
    private ArrayList<String> errors = new ArrayList<String>();
    private boolean multipleFiles;
    private boolean auto_clear_optional_fields;

    public static void main(String[] args) {
        
        AltJavaGenerator generator = new AltJavaGenerator();
        args = CommandLineSupport.setOptions(generator, args);
        
        if (args.length == 0) {
            System.out.println("No proto files specified.");
        }
        for (int i = 0; i < args.length; i++) {
            try {
                System.out.println("Compiling: "+args[i]);
                generator.compile(new File(args[i]));
            } catch (CompilerException e) {
                System.out.println("Protocol Buffer Compiler failed with the following error(s):");
                for (String error : e.getErrors() ) {
                    System.out.println("");
                    System.out.println(error);
                }
                System.out.println("");
                System.out.println("Compile failed.  For more details see error messages listed above.");
                return;
            }
        }

    }

    interface Closure {
        void execute() throws CompilerException;
    }
    
    public void compile(File file) throws CompilerException {

        // Parse the proto file
        FileInputStream is=null;
        try {
            is = new FileInputStream(file);
            ProtoParser parser = new ProtoParser(is);
            proto = parser.ProtoDescriptor();
            proto.setName(file.getName());
            loadImports(proto, file.getParentFile());
            proto.validate(errors);
        } catch (FileNotFoundException e) {
            errors.add("Failed to open: "+file.getPath()+":"+e.getMessage());
        } catch (ParseException e) {
            errors.add("Failed to parse: "+file.getPath()+":"+e.getMessage());
        } finally {
            try { is.close(); } catch (Throwable ignore){}
        }

        if (!errors.isEmpty()) {
            throw new CompilerException(errors);
        }

        // Load the options..
        javaPackage = javaPackage(proto);
        outerClassName = javaClassName(proto);
//        optimizeFor = getOption(proto.getOptions(), "optimize_for", "SPEED");
        multipleFiles = isMultipleFilesEnabled(proto);
//		deferredDecode = Boolean.parseBoolean(getOption(proto.getOptions(), "deferred_decode", "false"));
        auto_clear_optional_fields = Boolean.parseBoolean(getOption(proto.getOptions(), "auto_clear_optional_fields", "false"));
		
        if( multipleFiles ) {
            generateProtoFile();
        } else {
            writeFile(outerClassName, new Closure(){
                public void execute() throws CompilerException {
                    generateProtoFile();
                }
            });
        }
        
        if (!errors.isEmpty()) {
            throw new CompilerException(errors);
        }

    }

    private void writeFile(String className, Closure closure) throws CompilerException {
        PrintWriter oldWriter = w;
        // Figure out the java file name..
        File outputFile = out;
        if (javaPackage != null) {
            String packagePath = javaPackage.replace('.', '/');
            outputFile = new File(outputFile, packagePath);
        }
        outputFile = new File(outputFile, className + ".java");
        
        // Start writing the output file..
        outputFile.getParentFile().mkdirs();
        
        FileOutputStream fos=null;
        try {
            fos = new FileOutputStream(outputFile);
            w = new PrintWriter(fos);
            closure.execute();
            w.flush();
        } catch (FileNotFoundException e) {
            errors.add("Failed to write to: "+outputFile.getPath()+":"+e.getMessage());
        } finally {
            try { fos.close(); } catch (Throwable ignore){}
            w = oldWriter;
        }
    }

    private void loadImports(ProtoDescriptor proto, File protoDir) {
        LinkedHashMap<String,ProtoDescriptor> children = new LinkedHashMap<String,ProtoDescriptor>(); 
        for (String imp : proto.getImports()) {
            File file = new File(protoDir, imp);
            for (int i = 0; i < path.length && !file.exists(); i++) {
                file = new File(path[i], imp);
            } 
            if ( !file.exists() ) {
                errors.add("Cannot load import: "+imp);
            }
            
            FileInputStream is=null;
            try {
                is = new FileInputStream(file);
                ProtoParser parser = new ProtoParser(is);
                ProtoDescriptor child = parser.ProtoDescriptor();
                child.setName(file.getName());
                loadImports(child, file.getParentFile());
                children.put(imp, child);
            } catch (ParseException e) {
                errors.add("Failed to parse: "+file.getPath()+":"+e.getMessage());
            } catch (FileNotFoundException e) {
                errors.add("Failed to open: "+file.getPath()+":"+e.getMessage());
            } finally {
                try { is.close(); } catch (Throwable ignore){}
            }
        }
        proto.setImportProtoDescriptors(children);
    }


    private void generateProtoFile() throws CompilerException {
        if( multipleFiles ) {
            for (EnumDescriptor value : proto.getEnums().values()) {
                final EnumDescriptor o = value;
                String className = uCamel(o.getName());
                writeFile(className, new Closure(){
                    public void execute() throws CompilerException {
                        generateFileHeader();
                        generateEnum(o);
                    }
                });
            }
            for (MessageDescriptor value : proto.getMessages().values()) {
                final MessageDescriptor o = value;
                String className = uCamel(o.getName());
                writeFile(className, new Closure(){
                    public void execute() throws CompilerException {
                        generateFileHeader();
                        generateMessageBean(o);
                    }
                });
            }

        } else {
            generateFileHeader();

            p("public class " + outerClassName + " {");
            indent();

            for (EnumDescriptor enumType : proto.getEnums().values()) {
                generateEnum(enumType);
            }
            for (MessageDescriptor m : proto.getMessages().values()) {
                generateMessageBean(m);
            }

            unindent();
            p("}");
        }
    }

    private void generateFileHeader() {
        p("//");
        p("// Generated by protoc, do not edit by hand.");
        p("//");
        if (javaPackage != null) {
            p("package " + javaPackage + ";");
            p("");
        }
    }

    private void generateMessageBean(MessageDescriptor m) {
        
        String className = uCamel(m.getName());
        String beanClassName = className+"Bean";
        String bufferClassName = className+"Buffer";
        p();
        
        String staticOption = "static ";
        if( multipleFiles && m.getParent()==null ) {
            staticOption="";
        }

        String extendsClause = " extends org.apache.activemq.protobuf.PBMessage<"+className+"."+beanClassName+", "+className+"."+bufferClassName+">";
        for (EnumFieldDescriptor enumFeild : m.getAssociatedEnumFieldDescriptors()) {
            String name = uCamel(enumFeild.getParent().getName());
            name = name+"."+name+"Creatable";
            extendsClause += ", "+name; 
        }
        
        p(staticOption+"public interface " + className + extendsClause +" {");
        p();
        indent();
        
        for (EnumDescriptor enumType : m.getEnums().values()) {
            generateEnum(enumType);
        }

        // Generate the Nested Messages.
        for (MessageDescriptor subMessage : m.getMessages().values()) {
            generateMessageBean(subMessage);
        }

        // Generate the Group Messages
        for (FieldDescriptor field : m.getFields().values()) {
            if( field.isGroup() ) {
                generateMessageBean(field.getGroup());
            }
        }
        
        // Generate the field getters
        for (FieldDescriptor field : m.getFields().values()) {
            generateFieldGetterSignatures(field);
        }
        
        p("public "+beanClassName+" copy();");
        p("public "+bufferClassName+" freeze();");
        p("public java.lang.StringBuilder toString(java.lang.StringBuilder sb, String prefix);");

        
        p();
        p("static public final class "+beanClassName+" implements "+className+" {");
        p();
        indent();
        
        p(""+bufferClassName+" frozen;");
        p(""+beanClassName+" bean;");
        p();
        p("public "+beanClassName+"() {");
        indent();
        p("this.bean = this;");
        unindent();
        p("}");
        p();
        p("public "+beanClassName+"("+beanClassName+" copy) {");
        indent();
        p("this.bean = copy;");
        unindent();
        p("}");
        p();
        p("public "+beanClassName+" copy() {");
        indent();
        p("return new "+beanClassName+"(bean);");
        unindent();
        p("}");
        p();
                
        generateMethodFreeze(m, bufferClassName);
        
        p("private void copyCheck() {");
        indent();
        p("assert frozen==null : org.apache.activemq.protobuf.MessageBufferSupport.FORZEN_ERROR_MESSAGE;");
        p("if (bean != this) {");
        indent();
        p("copy(bean);");
        unindent();
        p("}");
        unindent();
        p("}");
        p();

        generateMethodCopyFromBean(m, beanClassName);

        // Generate the field accessors..
        for (FieldDescriptor field : m.getFields().values()) {
            generateFieldAccessor(beanClassName, field);
        }
        
        generateMethodToString(m);

        generateMethodMergeFromStream(m, beanClassName);

        generateBeanEquals(m, beanClassName);

        generateMethodMergeFromBean(m, className);

        generateMethodClear(m);
        
        generateReadWriteExternal(m);
        
        for (EnumFieldDescriptor enumFeild : m.getAssociatedEnumFieldDescriptors()) {
            String enumName = uCamel(enumFeild.getParent().getName());
            p("public "+enumName+" to"+enumName+"() {");
            indent();
            p("return "+enumName+"."+enumFeild.getName()+";");
            unindent();
            p("}");
            p();            
        }        

        unindent();
        p("}");
        p();

        p("static public final class "+bufferClassName+" implements org.apache.activemq.protobuf.MessageBuffer<"+className+"."+beanClassName+", "+className+"."+bufferClassName+">, "+className+" {");
        p();
        indent();
        
        p("private "+beanClassName+" bean;");
        p("private org.apache.activemq.protobuf.Buffer buffer;");
        p("private int size=-1;");
        p("private int hashCode;");
        p();
        p("private "+bufferClassName+"(org.apache.activemq.protobuf.Buffer buffer) {");
        indent();
        p("this.buffer = buffer;");
        unindent();
        p("}");
        p();
        p("private "+bufferClassName+"("+beanClassName+" bean) {");
        indent();
        p("this.bean = bean;");
        unindent();
        p("}");
        p();
        p("public "+beanClassName+" copy() {");
        indent();
        p("return bean().copy();");
        unindent();
        p("}");
        p();
        p("public "+bufferClassName+" freeze() {");
        indent();
        p("return this;");
        unindent();
        p("}");
        p();
        p("private "+beanClassName+" bean() {");
        indent();
        p("if (bean == null) {");
        indent();
        p("try {");
        indent();
        p("bean = new "+beanClassName+"().mergeUnframed(new org.apache.activemq.protobuf.CodedInputStream(buffer));");
        p("bean.frozen=this;");
        unindent();
        p("} catch (org.apache.activemq.protobuf.InvalidProtocolBufferException e) {");
        indent();
        p("throw new RuntimeException(e);");
        unindent();
        p("} catch (java.io.IOException e) {");
        indent();
        p("throw new RuntimeException(\"An IOException was thrown (should never happen in this method).\", e);");
        unindent();
        p("}");
        unindent();
        p("}");
        p("return bean;");
        unindent();
        p("}");
        p();
        
        p("public String toString() {");
        indent();
        p("return bean().toString();");
        unindent();
        p("}");
        p();
        p("public java.lang.StringBuilder toString(java.lang.StringBuilder sb, String prefix) {");
        indent();
        p("return bean().toString(sb, prefix);");
        unindent();
        p("}");
        p();

        for (FieldDescriptor field : m.getFields().values()) {
            generateBufferGetters(field);
        }

        generateMethodWrite(m);
        
        generateMethodSerializedSize(m);

        generateMethodParseFrom(m, bufferClassName, beanClassName);
        
        generateBufferEquals(m, bufferClassName);

        p("public boolean frozen() {");
        indent();
        p("return true;");
        unindent();
        p("}");

        for (EnumFieldDescriptor enumFeild : m.getAssociatedEnumFieldDescriptors()) {
            String enumName = uCamel(enumFeild.getParent().getName());
            p("public "+enumName+" to"+enumName+"() {");
            indent();
            p("return "+enumName+"."+enumFeild.getName()+";");
            unindent();
            p("}");
            p();            
        }        
        
        unindent();
        p("}");
        p();


//        generateMethodVisitor(m);
//        generateMethodType(m, className);
        
       
        unindent();
        p("}");
        p();

    }


    private void generateMethodFreeze(MessageDescriptor m, String bufferClassName) {
        p("public boolean frozen() {");
        indent();
        p("return frozen!=null;");
        unindent();
        p("}");
        p();
        p("public "+bufferClassName+" freeze() {");
        indent();
        p("if( frozen==null ) {");
        indent();
        p("frozen = new "+bufferClassName+"(bean);");
        p("assert deepFreeze();");
        unindent();
        p("}");
        p("return frozen;");
        unindent();
        p("}");
        p();
        p("private boolean deepFreeze() {");
        indent();
        p("frozen.serializedSizeUnframed();");
        p("return true;");
        unindent();
        p("}");
        p();

    }
    
    /**
     * @param m
     * @param className
     */
    private void generateMethodCopyFromBean(MessageDescriptor m, String className) {
        p("private void copy("+className+" other) {");
        indent();
        p("this.bean = this;");
        for (FieldDescriptor field : m.getFields().values()) {
            String lname = lCamel(field.getName());
            String type = field.getRule()==FieldDescriptor.REPEATED_RULE ? javaCollectionType(field):javaType(field);
            boolean primitive = field.getTypeDescriptor()==null || field.getTypeDescriptor().isEnum();
            if( field.isRepeated() ) {
                if( primitive ) {
                    p("this.f_" + lname + " = other.f_" + lname + ";");
                    p("if( this.f_" + lname + " !=null && !other.frozen()) {");
                    indent();
                    p("this.f_" + lname + " = new java.util.ArrayList<"+type+">(this.f_" + lname + ");");
                    unindent();
                    p("}");
                } else {
                    p("this.f_" + lname + " = other.f_" + lname + ";");
                    p("if( this.f_" + lname + " !=null) {");
                    indent();
                    p("this.f_" + lname + " = new java.util.ArrayList<"+type+">(other.f_" + lname + ".size());");
                    p("for( "+type+" e :  other.f_" + lname + ") {");
                    indent();
                    p("this.f_" + lname + ".add(e.copy());");
                    unindent();
                    p("}");
                    unindent();
                    p("}");
                }
            } else {
                if( primitive ) {
                    p("this.f_" + lname + " = other.f_" + lname + ";");
                    p("this.b_" + lname + " = other.b_" + lname + ";");
                } else {
                    p("this.f_" + lname + " = other.f_" + lname + ";");
                    p("if( this.f_" + lname + " !=null ) {");
                    indent();
                    p("this.f_" + lname + " = this.f_" + lname + ".copy();");
                    unindent();
                    p("}");
                }
            }
        }
        unindent();
        p("}");
        p();
    }


	/**
     * If the java_visitor message option is set, then this method generates a visitor method.  The option 
     * speifiies the class name of the visitor and optionally the return value and exceptions thrown by the visitor.
     * 
     * Examples:
     * 
     *   option java_visitor = "org.apache.kahadb.store.Visitor";
     *   generates:
     *     public void visit(org.apache.kahadb.store.Visitor visitor) {
     *       visitor.visit(this);
     *     }
     *   
     *   option java_visitor = "org.apache.kahadb.store.Visitor:int:java.io.IOException";
     *   generates:
     *     public int visit(org.apache.kahadb.store.Visitor visitor) throws java.io.IOException {
     *       return visitor.visit(this);
     *     }
     * 
     * @param m
     */
    private void generateMethodVisitor(MessageDescriptor m) {
        String javaVisitor = getOption(m.getOptions(), "java_visitor", null);        
        if( javaVisitor!=null ) {
            String returns = "void";
            String throwsException = null;
            
            StringTokenizer st = new StringTokenizer(javaVisitor, ":");
            String vistorClass = st.nextToken();
            if( st.hasMoreTokens() ) {
                returns = st.nextToken();
            }
            if( st.hasMoreTokens() ) {
                throwsException = st.nextToken();
            }
            
            String throwsClause = "";
            if( throwsException!=null ) {
                throwsClause = "throws "+throwsException+" ";
            }
            
            p("public "+returns+" visit("+vistorClass+" visitor) "+throwsClause+ "{");
            indent();
            if( "void".equals(returns) ) {
                p("visitor.visit(this);");
            } else {
                p("return visitor.visit(this);");
            }
            unindent();
            p("}");
            p();
        }
    }
    
    private void generateMethodType(MessageDescriptor m, String className) {
        String typeEnum = getOption(m.getOptions(), "java_type_method", null);        
        if( typeEnum!=null ) {
            
            TypeDescriptor typeDescriptor = m.getType(typeEnum);
            if( typeDescriptor == null ) {
                typeDescriptor = m.getProtoDescriptor().getType(typeEnum);
            }
            if( typeDescriptor == null || !typeDescriptor.isEnum() ) {
                errors.add("The java_type_method option on the "+m.getName()+" message does not point to valid enum type");
                return;
            }
            
            
            String constant = constantCase(className);
            EnumDescriptor enumDescriptor = (EnumDescriptor)typeDescriptor;
            if( enumDescriptor.getFields().get(constant) == null ) {
                errors.add("The java_type_method option on the "+m.getName()+" message does not points to the "+typeEnum+" enum but it does not have an entry for "+constant);
            }
            
            String type = javaType(typeDescriptor);
            
            p("public "+type+" type() {");
            indent();
                p("return "+type+"."+constant+";");
            unindent();
            p("}");
            p();
        }
    }
    
    private void generateMethodParseFrom(MessageDescriptor m, String bufferClassName, String beanClassName) {
        p("public static "+beanClassName+" parseUnframed(org.apache.activemq.protobuf.CodedInputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return new "+beanClassName+"().mergeUnframed(data);");
        unindent();
        p("}");
        p();
        p("public static "+beanClassName+" parseUnframed(java.io.InputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return parseUnframed(new org.apache.activemq.protobuf.CodedInputStream(data));");
        unindent();
        p("}");
        p();
        p("public static "+bufferClassName+" parseUnframed(org.apache.activemq.protobuf.Buffer data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return new "+bufferClassName+"(data);");
        unindent();
        p("}");
        p();
        p("public static "+bufferClassName+" parseUnframed(byte[] data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return parseUnframed(new org.apache.activemq.protobuf.Buffer(data));");
        unindent();
        p("}");
        p();
        p("public static "+bufferClassName+" parseFramed(org.apache.activemq.protobuf.CodedInputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("int length = data.readRawVarint32();");
        p("int oldLimit = data.pushLimit(length);");
        p(""+bufferClassName+" rc = parseUnframed(data.readRawBytes(length));");
        p("data.popLimit(oldLimit);");
        p("return rc;");
        unindent();
        p("}");
        p();
        p("public static "+bufferClassName+" parseFramed(org.apache.activemq.protobuf.Buffer data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("try {");
        indent();
        p("org.apache.activemq.protobuf.CodedInputStream input = new org.apache.activemq.protobuf.CodedInputStream(data);");
        p(""+bufferClassName+" rc = parseFramed(input);");
        p("input.checkLastTagWas(0);");
        p("return rc;");
        unindent();
        p("} catch (org.apache.activemq.protobuf.InvalidProtocolBufferException e) {");
        indent();
        p("throw e;");
        unindent();
        p("} catch (java.io.IOException e) {");
        indent();
        p("throw new RuntimeException(\"An IOException was thrown (should never happen in this method).\", e);");
        unindent();
        p("}");
        unindent();
        p("}");
        p();
        p("public static "+bufferClassName+" parseFramed(byte[] data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return parseFramed(new org.apache.activemq.protobuf.Buffer(data));");
        unindent();
        p("}");
        p();
        p("public static "+bufferClassName+" parseFramed(java.io.InputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return parseUnframed(org.apache.activemq.protobuf.MessageBufferSupport.readFrame(data));");
        unindent();
        p("}");
        p();
        
    }

    private void generateBeanEquals(MessageDescriptor m, String className) {
        p("public boolean equals(Object obj) {");
        indent();
        p("if( obj==this )");
        p("   return true;");
        p("");
        p("if( obj==null || obj.getClass()!="+className+".class )");
        p("   return false;");
        p("");
        p("return equals(("+className+")obj);");
        unindent();
        p("}");
        p("");
        
        p("public boolean equals("+className+" obj) {");
        indent();
        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            String getterMethod="get"+uname+"()";     
            String hasMethod = "has"+uname+"()";

            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                getterMethod = "get"+uname+"List()";
            }
            
            p("if ("+hasMethod+" ^ obj."+hasMethod+" ) ");
            p("   return false;");
            
            
            
            if( field.getRule() != FieldDescriptor.REPEATED_RULE && (field.isNumberType() || field.getType()==FieldDescriptor.BOOL_TYPE) ) {
                p("if ("+hasMethod+" && ( "+getterMethod+"!=obj."+getterMethod+" ))");
            } else {
                p("if ("+hasMethod+" && ( !"+getterMethod+".equals(obj."+getterMethod+") ))");
            }
            p("   return false;");
        }
        p("return true;");
        unindent();
        p("}");
        p("");
        p("public int hashCode() {");
        indent();
        int hc = className.hashCode();
        p("int rc="+hc+";");
        int counter=0;
        for (FieldDescriptor field : m.getFields().values()) {
        	counter++;
        	
            String uname = uCamel(field.getName());
            String getterMethod="get"+uname+"()";     
            String hasMethod = "has"+uname+"()";

            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                getterMethod = "get"+uname+"List()";
            }
            
            p("if ("+hasMethod+") {");
            indent();
            
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                p("rc ^= ( "+uname.hashCode()+"^"+getterMethod+".hashCode() );");
            } else if( field.isInteger32Type() ) {
                p("rc ^= ( "+uname.hashCode()+"^"+getterMethod+" );");
            } else if( field.isInteger64Type() ) {
                p("rc ^= ( "+uname.hashCode()+"^(new Long("+getterMethod+")).hashCode() );");
            } else if( field.getType()==FieldDescriptor.DOUBLE_TYPE ) {
                p("rc ^= ( "+uname.hashCode()+"^(new Double("+getterMethod+")).hashCode() );");
            } else if( field.getType()==FieldDescriptor.FLOAT_TYPE ) {
                p("rc ^= ( "+uname.hashCode()+"^(new Double("+getterMethod+")).hashCode() );");
            } else if( field.getType()==FieldDescriptor.BOOL_TYPE ) {
                p("rc ^= ( "+uname.hashCode()+"^ ("+getterMethod+"? "+counter+":-"+counter+") );");
            } else {
                p("rc ^= ( "+uname.hashCode()+"^"+getterMethod+".hashCode() );");
            }
            unindent();
            p("}");
        }
        p("return rc;");
        unindent();
        p("}");
        p("");
	}
    
    private void generateBufferEquals(MessageDescriptor m, String className) {
        p("public boolean equals(Object obj) {");
        indent();
        p("if( obj==this )");
        p("   return true;");
        p("");
        p("if( obj==null || obj.getClass()!="+className+".class )");
        p("   return false;");
        p("");
        p("return equals(("+className+")obj);");
        unindent();
        p("}");
        p("");
        
        p("public boolean equals("+className+" obj) {");
        indent();
        p("return toUnframedBuffer().equals(obj.toUnframedBuffer());");
        unindent();
        p("}");
        p("");
        p("public int hashCode() {");
        indent();
        int hc = className.hashCode();
        p("if( hashCode==0 ) {");
        p("hashCode="+hc+" ^ toUnframedBuffer().hashCode();");
        p("}");
        p("return hashCode;");
        unindent();
        p("}");
        p("");
    }

    /**
     * @param m
     */
    private void generateMethodSerializedSize(MessageDescriptor m) {
        
        p("public int serializedSizeFramed() {");
        indent();
        p("int t = serializedSizeUnframed();");
        p("return org.apache.activemq.protobuf.CodedOutputStream.computeRawVarint32Size(t) + t;");
        unindent();
        p("}");
        p();
        p("public int serializedSizeUnframed() {");
        indent();
		p("if (buffer != null) {");
		indent();
        p("return buffer.length;");
		unindent();
		p("}");
        p("if (size != -1)");
        p("   return size;");
        p();
        p("size = 0;");
        for (FieldDescriptor field : m.getFields().values()) {
            
            String uname = uCamel(field.getName());
            String getter="get"+uname+"()";            
            String type = javaType(field);
            
            if( !field.isRequired() ) {
                p("if (has"+uname+"()) {");
                indent();
            }
            
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                p("for ("+type+" i : get"+uname+"List()) {");
                indent();
                getter = "i";
            }

            if( field.getType()==FieldDescriptor.STRING_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeStringSize("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.BYTES_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeBytesSize("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.BOOL_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeBoolSize("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.DOUBLE_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeDoubleSize("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.FLOAT_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeFloatSize("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.INT32_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeInt32Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.INT64_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeInt64Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SINT32_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeSInt32Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SINT64_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeSInt64Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.UINT32_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeUInt32Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.UINT64_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeUInt64Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.FIXED32_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeFixed32Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.FIXED64_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeFixed64Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SFIXED32_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeSFixed32Size("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SFIXED64_TYPE ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeSFixed64Size("+field.getTag()+", "+getter+");");
            } else if( field.getTypeDescriptor().isEnum() ) {
                p("size += org.apache.activemq.protobuf.CodedOutputStream.computeEnumSize("+field.getTag()+", "+getter+".getNumber());");
            } else if ( field.getGroup()!=null ) {
                errors.add("This code generator does not support group fields.");
//                p("size += org.apache.activemq.protobuf.MessageBufferSupport.computeGroupSize("+field.getTag()+", ("+type+"Buffer)"+getter+");");
            } else {
                p("size += org.apache.activemq.protobuf.MessageBufferSupport.computeMessageSize("+field.getTag()+", "+getter+".freeze());");
            }
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                unindent();
                p("}");
            }
            
            if( !field.isRequired() ) {
                unindent();
                p("}");
            }

        }
        // TODO: handle unknown fields
        // size += getUnknownFields().getSerializedSize();");
        p("return size;");
        unindent();
        p("}");
        p();
    }

    /**
     * @param m
     */
    private void generateMethodWrite(MessageDescriptor m) {
        
        p("public org.apache.activemq.protobuf.Buffer toUnframedBuffer() {");
        indent();
        p("if( buffer !=null ) {");
        indent();
        p("return buffer;");
        unindent();
        p("}");
        p("return org.apache.activemq.protobuf.MessageBufferSupport.toUnframedBuffer(this);");
        unindent();
        p("}");
        p();
        p("public org.apache.activemq.protobuf.Buffer toFramedBuffer() {");
        indent();
        p("return org.apache.activemq.protobuf.MessageBufferSupport.toFramedBuffer(this);");
        unindent();
        p("}");
        p();
        p("public byte[] toUnframedByteArray() {");
        indent();
        p("return toUnframedBuffer().toByteArray();");
        unindent();
        p("}");
        p();
        p("public byte[] toFramedByteArray() {");
        indent();
        p("return toFramedBuffer().toByteArray();");
        unindent();
        p("}");
        p();
        p("public void writeFramed(org.apache.activemq.protobuf.CodedOutputStream output) throws java.io.IOException {");
        indent();
        p("output.writeRawVarint32(serializedSizeUnframed());");
        p("writeUnframed(output);");
        unindent();
        p("}");
        p();
        p("public void writeFramed(java.io.OutputStream output) throws java.io.IOException {");
        indent();
        p("org.apache.activemq.protobuf.CodedOutputStream codedOutput = new org.apache.activemq.protobuf.CodedOutputStream(output);");
        p("writeFramed(codedOutput);");
        p("codedOutput.flush();");
        unindent();
        p("}");
        p();
        
        p("public void writeUnframed(java.io.OutputStream output) throws java.io.IOException {");
        indent();
        p("org.apache.activemq.protobuf.CodedOutputStream codedOutput = new org.apache.activemq.protobuf.CodedOutputStream(output);");
        p("writeUnframed(codedOutput);");
        p("codedOutput.flush();");
        unindent();
        p("}");
        p();

        p("public void writeUnframed(org.apache.activemq.protobuf.CodedOutputStream output) throws java.io.IOException {");
        indent();
        
		p("if (buffer == null) {");
		indent();
        p("int size = serializedSizeUnframed();");
        p("buffer = output.getNextBuffer(size);");
        p("org.apache.activemq.protobuf.CodedOutputStream original=null;");
        p("if( buffer == null ) {");
        indent();
        p("buffer = new org.apache.activemq.protobuf.Buffer(new byte[size]);");
        p("original = output;");
        p("output = new org.apache.activemq.protobuf.CodedOutputStream(buffer);");
        unindent();
        p("}");

        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            String getter="bean.get"+uname+"()";            
            String type = javaType(field);
            
            if( !field.isRequired() ) {
                p("if (bean.has"+uname+"()) {");
                indent();
            }
            
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                p("for ("+type+" i : bean.get"+uname+"List()) {");
                indent();
                getter = "i";
            }

            if( field.getType()==FieldDescriptor.STRING_TYPE ) {
                p("output.writeString("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.BYTES_TYPE ) {
                p("output.writeBytes("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.BOOL_TYPE ) {
                p("output.writeBool("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.DOUBLE_TYPE ) {
                p("output.writeDouble("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.FLOAT_TYPE ) {
                p("output.writeFloat("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.INT32_TYPE ) {
                p("output.writeInt32("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.INT64_TYPE ) {
                p("output.writeInt64("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SINT32_TYPE ) {
                p("output.writeSInt32("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SINT64_TYPE ) {
                p("output.writeSInt64("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.UINT32_TYPE ) {
                p("output.writeUInt32("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.UINT64_TYPE ) {
                p("output.writeUInt64("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.FIXED32_TYPE ) {
                p("output.writeFixed32("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.FIXED64_TYPE ) {
                p("output.writeFixed64("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SFIXED32_TYPE ) {
                p("output.writeSFixed32("+field.getTag()+", "+getter+");");
            } else if( field.getType()==FieldDescriptor.SFIXED64_TYPE ) {
                p("output.writeSFixed64("+field.getTag()+", "+getter+");");
            } else if( field.getTypeDescriptor().isEnum() ) {
                p("output.writeEnum("+field.getTag()+", "+getter+".getNumber());");
            } else if ( field.getGroup()!=null ) {
                errors.add("This code generator does not support group fields.");
//                p("writeGroup(output, "+field.getTag()+", "+getter+");");
            } else {
                p("org.apache.activemq.protobuf.MessageBufferSupport.writeMessage(output, "+field.getTag()+", "+getter+".freeze());");
            }
            
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                unindent();
                p("}");
            }
            
            if( !field.isRequired() ) {
                unindent();
                p("}");
            }
        }
        
        p("if( original !=null ) {");
        indent();
        p("output.checkNoSpaceLeft();");
        p("output = original;");
        p("output.writeRawBytes(buffer);");
        unindent();
        p("}");
        unindent();
        p("} else {");
        indent();
        p("output.writeRawBytes(buffer);");
        unindent();
        p("}");

        unindent();
        p("}");
        p();        
    }

    /**
     * @param m
     * @param className
     */
    private void generateMethodMergeFromStream(MessageDescriptor m, String className) {
        p("public "+className+" mergeUnframed(java.io.InputStream input) throws java.io.IOException {");
        indent();
        p("return mergeUnframed(new org.apache.activemq.protobuf.CodedInputStream(input));");
        unindent();
        p("}");
        p();
        p("public "+className+" mergeUnframed(org.apache.activemq.protobuf.CodedInputStream input) throws java.io.IOException {");
        indent();
		{
            p("copyCheck();");
			p("while (true) {");
			indent();
			{
				p("int tag = input.readTag();");
				p("if ((tag & 0x07) == 4) {");
				p("   return this;");
				p("}");

				p("switch (tag) {");
				p("case 0:");
				p("   return this;");
				p("default: {");

				p("   break;");
				p("}");

				for (FieldDescriptor field : m.getFields().values()) {
					String uname = uCamel(field.getName());
					String setter = "set" + uname;
					boolean repeated = field.getRule() == FieldDescriptor.REPEATED_RULE;
					if (repeated) {
						setter = "create" + uname + "List().add";
					}
					if (field.getType() == FieldDescriptor.STRING_TYPE) {
						p("case "
								+ makeTag(field.getTag(),
										WIRETYPE_LENGTH_DELIMITED) + ":");
						indent();
						p(setter + "(input.readString());");
					} else if (field.getType() == FieldDescriptor.BYTES_TYPE) {
						p("case "+ makeTag(field.getTag(), WIRETYPE_LENGTH_DELIMITED) + ":");
						indent();
			            String override = getOption(field.getOptions(), "java_override_type", null);
			            if( "AsciiBuffer".equals(override) ) {
                            p(setter + "(new org.apache.activemq.protobuf.AsciiBuffer(input.readBytes()));");
			            } else if( "UTF8Buffer".equals(override) ) {
                            p(setter + "(new org.apache.activemq.protobuf.UTF8Buffer(input.readBytes()));");
			            } else {
	                        p(setter + "(input.readBytes());");
			            }						
					} else if (field.getType() == FieldDescriptor.BOOL_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)+ ":");
						indent();
						p(setter + "(input.readBool());");
					} else if (field.getType() == FieldDescriptor.DOUBLE_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_FIXED64)
								+ ":");
						indent();
						p(setter + "(input.readDouble());");
					} else if (field.getType() == FieldDescriptor.FLOAT_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_FIXED32)
								+ ":");
						indent();
						p(setter + "(input.readFloat());");
					} else if (field.getType() == FieldDescriptor.INT32_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						p(setter + "(input.readInt32());");
					} else if (field.getType() == FieldDescriptor.INT64_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						p(setter + "(input.readInt64());");
					} else if (field.getType() == FieldDescriptor.SINT32_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						p(setter + "(input.readSInt32());");
					} else if (field.getType() == FieldDescriptor.SINT64_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						p(setter + "(input.readSInt64());");
					} else if (field.getType() == FieldDescriptor.UINT32_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						p(setter + "(input.readUInt32());");
					} else if (field.getType() == FieldDescriptor.UINT64_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						p(setter + "(input.readUInt64());");
					} else if (field.getType() == FieldDescriptor.FIXED32_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_FIXED32)
								+ ":");
						indent();
						p(setter + "(input.readFixed32());");
					} else if (field.getType() == FieldDescriptor.FIXED64_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_FIXED64)
								+ ":");
						indent();
						p(setter + "(input.readFixed64());");
					} else if (field.getType() == FieldDescriptor.SFIXED32_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_FIXED32)
								+ ":");
						indent();
						p(setter + "(input.readSFixed32());");
					} else if (field.getType() == FieldDescriptor.SFIXED64_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_FIXED64)
								+ ":");
						indent();
						p(setter + "(input.readSFixed64());");
					} else if (field.getTypeDescriptor().isEnum()) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
						indent();
						String type = javaType(field);
						p("{");
						indent();
						p("int t = input.readEnum();");
						p("" + type + " value = " + type + ".valueOf(t);");
						p("if( value !=null ) {");
						indent();
						p(setter + "(value);");
						unindent();
						p("}");
						// TODO: else store it as an known

						unindent();
						p("}");

					} else if (field.getGroup() != null) {
					    errors.add("This code generator does not support group fields.");
//						p("case "+ makeTag(field.getTag(), WIRETYPE_START_GROUP)+ ":");
//						indent();
//						String type = javaType(field);
//						if (repeated) {
//							p(setter + "(readGroup(input, " + field.getTag()+ ", new " + type + "()));");
//						} else {
//							p("if (has" + uname + "()) {");
//							indent();
//							p("readGroup(input, " + field.getTag() + ", get"
//									+ uname + "());");
//							unindent();
//							p("} else {");
//							indent();
//							p(setter + "(readGroup(input, " + field.getTag()
//									+ ",new " + type + "()));");
//							unindent();
//							p("}");
//						}
//						p("");
					} else {
						p("case "+ makeTag(field.getTag(),WIRETYPE_LENGTH_DELIMITED) + ":");
						indent();
						String type = javaType(field);
						if (repeated) {
                          p(setter + "("+javaRelatedType(type, "Buffer")+".parseFramed(input));");
						} else {
							p("if (has" + uname + "()) {");
							indent();
                            p("set" + uname + "(get" + uname + "().copy().mergeFrom("+javaRelatedType(type, "Buffer")+".parseFramed(input)));");
							unindent();
							p("} else {");
							indent();
                            p(setter + "("+javaRelatedType(type, "Buffer")+".parseFramed(input));");
							unindent();
							p("}");
						}
					}
					p("break;");
					unindent();
				}
				p("}");
			}
			unindent();
			p("}");
		}
		unindent();
		p("}");
    }
   
    /**
	 * @param m
	 * @param className
	 */
    private void generateMethodMergeFromBean(MessageDescriptor m, String className) {
        p("public "+className+"Bean mergeFrom("+className+" other) {");
        indent();
        p("copyCheck();");
        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            p("if (other.has"+uname+"()) {");
            indent();

            if( field.isScalarType() || field.getTypeDescriptor().isEnum() ) {
                if( field.isRepeated() ) {
                    p("get"+uname+"List().addAll(other.get"+uname+"List());");
                } else {
                    p("set"+uname+"(other.get"+uname+"());");
                }
            } else {
                
                String type = javaType(field);
                // It's complex type...
                if( field.isRepeated() ) {
                    p("for("+type+" element: other.get"+uname+"List() ) {");
                    indent();
                        p("get"+uname+"List().add(element.copy());");
                    unindent();
                    p("}");
                } else {
                    p("if (has"+uname+"()) {");
                    indent();
                    p("set"+uname+"(get"+uname+"().copy().mergeFrom(other.get"+uname+"()));");
                    unindent();
                    p("} else {");
                    indent();
                    p("set"+uname+"(other.get"+uname+"().copy());");
                    unindent();
                    p("}");
                }
            }
            unindent();
            p("}");
        }
        p("return this;");
        unindent();
        p("}");
        p();
    }

    /**
	 * @param m
	 */
    private void generateMethodClear(MessageDescriptor m) {
        p("public void clear() {");
        indent();
        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            p("clear" + uname + "();");
        }
        unindent();
        p("}");
        p();
    }
    
    private void generateReadWriteExternal(MessageDescriptor m) {
        
        p("public void readExternal(java.io.DataInput in) throws java.io.IOException {");
        indent();
        p("assert frozen==null : org.apache.activemq.protobuf.MessageBufferSupport.FORZEN_ERROR_MESSAGE;");
        p("bean = this;");
        p("frozen = null;");
        
        for (FieldDescriptor field : m.getFields().values()) {
            String lname = lCamel(field.getName());
            String type = javaType(field);
            boolean repeated = field.getRule()==FieldDescriptor.REPEATED_RULE;
    
            // Create the fields..
            if( repeated ) {
                p("{");
                indent();
                p("int size = in.readShort();");
                p("if( size>=0 ) {");
                indent();
                p("f_"+lname+" = new java.util.ArrayList<" + javaCollectionType(field) + ">(size);");
                p("for(int i=0; i<size; i++) {");
                indent();
                if( field.isInteger32Type() ) {
                    p("f_"+lname+".add(in.readInt());");
                } else if( field.isInteger64Type() ) {
                    p("f_"+lname+".add(in.readLong());");
                } else if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
                    p("f_"+lname+".add(in.readDouble());");
                } else if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
                    p("f_"+lname+".add(in.readFloat());");
                } else if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
                    p("f_"+lname+".add(in.readBoolean());");
                } else if( field.getType() == FieldDescriptor.STRING_TYPE ) {
                    p("f_"+lname+".add(in.readUTF());");
                } else if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
                    p("byte b[] = new byte[in.readInt()];");
                    p("in.readFully(b);");
                    p("f_"+lname+".add(new "+type+"(b));");
                } else if (field.getTypeDescriptor().isEnum() ) {
                    p("f_"+lname+".add(" + type + ".valueOf(in.readShort()));");
                } else {
                    p(""+javaRelatedType(type, "Bean")+" o = new "+javaRelatedType(type, "Bean")+"();");
                    p("o.readExternal(in);");
                    p("f_"+lname+".add(o);");
                }
                unindent();
                p("}");
                unindent();
                p("} else {");
                indent();
                p("f_"+lname+" = null;");
                unindent();
                p("}");
                unindent();
                p("}");

            } else {
                if( field.isInteger32Type() ) {
                    p("f_"+lname+" = in.readInt();");
                    p("b_"+lname+" = true;");
                } else if( field.isInteger64Type() ) {
                    p("f_"+lname+" = in.readLong();");
                    p("b_"+lname+" = true;");
                } else if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
                    p("f_"+lname+" = in.readDouble();");
                    p("b_"+lname+" = true;");
                } else if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
                    p("f_"+lname+" = in.readFloat();");
                    p("b_"+lname+" = true;");
                } else if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
                    p("f_"+lname+" = in.readBoolean();");
                    p("b_"+lname+" = true;");
                } else if( field.getType() == FieldDescriptor.STRING_TYPE ) {
                    p("if( in.readBoolean() ) {");
                    indent();
                    p("f_"+lname+" = in.readUTF();");
                    p("b_"+lname+" = true;");
                    unindent();
                    p("} else {");
                    indent();
                    p("f_"+lname+" = null;");
                    p("b_"+lname+" = false;");
                    unindent();
                    p("}");
                } else if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
                    p("{");
                    indent();
                    p("int size = in.readInt();");
                    p("if( size>=0 ) {");
                    indent();
                    p("byte b[] = new byte[size];");
                    p("in.readFully(b);");
                    p("f_"+lname+" = new "+type+"(b);");
                    p("b_"+lname+" = true;");
                    unindent();
                    p("} else {");
                    indent();
                    p("f_"+lname+" = null;");
                    p("b_"+lname+" = false;");
                    unindent();
                    p("}");
                    unindent();
                    p("}");
                } else if (field.getTypeDescriptor().isEnum() ) {
                    p("if( in.readBoolean() ) {");
                    indent();
                    p("f_"+lname+" = " + type + ".valueOf(in.readShort());");
                    p("b_"+lname+" = true;");
                    unindent();
                    p("} else {");
                    indent();
                    p("f_"+lname+" = null;");
                    p("b_"+lname+" = false;");
                    unindent();
                    p("}");
                } else {
                    p("if( in.readBoolean() ) {");
                    indent();
                    p(""+javaRelatedType(type, "Bean")+" o = new "+javaRelatedType(type, "Bean")+"();");
                    p("o.readExternal(in);");
                    p("f_"+lname+" = o;");
                    unindent();
                    p("} else {");
                    indent();
                    p("f_"+lname+" = null;");
                    unindent();
                    p("}");                    
                }
            }
        }

        unindent();
        p("}");
        p();
        p("public void writeExternal(java.io.DataOutput out) throws java.io.IOException {");
        indent();
        for (FieldDescriptor field : m.getFields().values()) {
            String lname = lCamel(field.getName());
            boolean repeated = field.getRule()==FieldDescriptor.REPEATED_RULE;
    
            // Create the fields..
            if( repeated ) {
                p("if( bean.f_"+lname+"!=null ) {");
                indent();
                p("out.writeShort(bean.f_"+lname+".size());");
                p("for(" + javaCollectionType(field) + " o : bean.f_"+lname+") {");
                indent();
                
                if( field.isInteger32Type() ) {
                    p("out.writeInt(o);");
                } else if( field.isInteger64Type() ) {
                    p("out.writeLong(o);");
                } else if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
                    p("out.writeDouble(o);");
                } else if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
                    p("out.writeFloat(o);");
                } else if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
                    p("out.writeBoolean(o);");
                } else if( field.getType() == FieldDescriptor.STRING_TYPE ) {
                    p("out.writeUTF(o);");
                } else if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
                    p("out.writeInt(o.getLength());");
                    p("out.write(o.getData(), o.getOffset(), o.getLength());");
                } else if (field.getTypeDescriptor().isEnum() ) {
                    p("out.writeShort(o.getNumber());");
                } else {
                    p("o.copy().writeExternal(out);");
                }
                unindent();
                p("}");
                unindent();
                p("} else {");
                indent();
                p("out.writeShort(-1);");
                unindent();
                p("}");

            } else {
                if( field.isInteger32Type() ) {
                    p("out.writeInt(bean.f_"+lname+");");
                } else if( field.isInteger64Type() ) {
                    p("out.writeLong(bean.f_"+lname+");");
                } else if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
                    p("out.writeDouble(bean.f_"+lname+");");
                } else if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
                    p("out.writeFloat(bean.f_"+lname+");");
                } else if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
                    p("out.writeBoolean(bean.f_"+lname+");");
                } else if( field.getType() == FieldDescriptor.STRING_TYPE ) {
                    p("if( bean.f_"+lname+"!=null ) {");
                    indent();
                    p("out.writeBoolean(true);");
                    p("out.writeUTF(bean.f_"+lname+");");
                    unindent();
                    p("} else {");
                    indent();
                    p("out.writeBoolean(false);");
                    unindent();
                    p("}");
                } else if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
                    p("if( bean.f_"+lname+"!=null ) {");
                    indent();
                    p("out.writeInt(bean.f_"+lname+".getLength());");
                    p("out.write(bean.f_"+lname+".getData(), bean.f_"+lname+".getOffset(), bean.f_"+lname+".getLength());");
                    unindent();
                    p("} else {");
                    indent();
                    p("out.writeInt(-1);");
                    unindent();
                    p("}");
                } else if (field.getTypeDescriptor().isEnum() ) {
                    p("if( bean.f_"+lname+"!=null ) {");
                    indent();
                    p("out.writeBoolean(true);");
                    p("out.writeShort(bean.f_"+lname+".getNumber());");
                    unindent();
                    p("} else {");
                    indent();
                    p("out.writeBoolean(false);");
                    unindent();
                    p("}");
                } else {
                    p("if( bean.f_"+lname+"!=null ) {");
                    indent();
                    p("out.writeBoolean(true);");
                    p("bean.f_"+lname+".copy().writeExternal(out);");
                    unindent();
                    p("} else {");
                    indent();
                    p("out.writeBoolean(false);");
                    unindent();
                    p("}");
                }
            }
        }
        
        unindent();
        p("}");
        p();            
        
    }
    

//    private void generateMethodAssertInitialized(MessageDescriptor m, String className) {
//        
//        p("public java.util.ArrayList<String> missingFields() {");
//        indent();
//        p("java.util.ArrayList<String> missingFields = super.missingFields();");
//        
//        for (FieldDescriptor field : m.getFields().values()) {
//            String uname = uCamel(field.getName());
//            if( field.isRequired() ) {
//                p("if(  !has" + uname + "() ) {");
//                indent();
//                p("missingFields.add(\""+field.getName()+"\");");
//                unindent();
//                p("}");
//            }
//        }
//        
//        if( !deferredDecode ) {
//	        for (FieldDescriptor field : m.getFields().values()) {
//	            if( field.getTypeDescriptor()!=null && !field.getTypeDescriptor().isEnum()) {
//	                String uname = uCamel(field.getName());
//	                p("if( has" + uname + "() ) {");
//	                indent();
//	                if( !field.isRepeated() ) {
//	                    p("try {");
//	                    indent();
//	                    p("get" + uname + "().assertInitialized();");
//	                    unindent();
//	                    p("} catch (org.apache.activemq.protobuf.UninitializedMessageException e){");
//	                    indent();
//	                    p("missingFields.addAll(prefix(e.getMissingFields(),\""+field.getName()+".\"));");
//	                    unindent();
//	                    p("}");
//	                } else {
//	                    String type = javaCollectionType(field);
//	                    p("java.util.List<"+type+"> l = get" + uname + "List();");
//	                    p("for( int i=0; i < l.size(); i++ ) {");
//	                    indent();
//	                    p("try {");
//	                    indent();
//	                    p("l.get(i).assertInitialized();");
//	                    unindent();
//	                    p("} catch (org.apache.activemq.protobuf.UninitializedMessageException e){");
//	                    indent();
//	                    p("missingFields.addAll(prefix(e.getMissingFields(),\""+field.getName()+"[\"+i+\"]\"));");
//	                    unindent();
//	                    p("}");
//	                    unindent();
//	                    p("}");
//	                }
//	                unindent();
//	                p("}");
//	            }
//	        }
//        }
//        p("return missingFields;");
//        unindent();
//        p("}");
//        p();
//    }

    private void generateMethodToString(MessageDescriptor m) {
        
        p("public String toString() {");
        indent();
        p("return toString(new java.lang.StringBuilder(), \"\").toString();");
        unindent();
        p("}");
        p();

        p("public java.lang.StringBuilder toString(java.lang.StringBuilder sb, String prefix) {");
        indent();
        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            p("if(  has" + uname + "() ) {");
            indent();
            if( field.isRepeated() ) {
                String type = javaCollectionType(field);
                p("java.util.List<"+type+"> l = get" + uname + "List();");
                p("for( int i=0; i < l.size(); i++ ) {");
                indent();
                if( field.getTypeDescriptor()!=null && !field.getTypeDescriptor().isEnum()) {
                    p("sb.append(prefix+\""+field.getName()+"[\"+i+\"] {\\n\");");
                    p("l.get(i).toString(sb, prefix+\"  \");");
                    p("sb.append(prefix+\"}\\n\");");
                } else {
                    p("sb.append(prefix+\""+field.getName()+"[\"+i+\"]: \");");
                    p("sb.append(l.get(i));");
                    p("sb.append(\"\\n\");");
                }
                unindent();
                p("}");
            } else {
                if( field.getTypeDescriptor()!=null && !field.getTypeDescriptor().isEnum()) {
                    p("sb.append(prefix+\""+field.getName()+" {\\n\");");
                    p("get" + uname + "().toString(sb, prefix+\"  \");");
                    p("sb.append(prefix+\"}\\n\");");
                } else {
                    p("sb.append(prefix+\""+field.getName()+": \");");
                    p("sb.append(get" + uname + "());");
                    p("sb.append(\"\\n\");");
                }
            }
            unindent();
            p("}");
        }
        p("return sb;");
        unindent();
        p("}");
        p();

    }
    
    /**
     * @param field
     * @param className 
     */
    private void generateBufferGetters(FieldDescriptor field) {
        String uname = uCamel(field.getName());
        String type = field.getRule()==FieldDescriptor.REPEATED_RULE ? javaCollectionType(field):javaType(field);
        boolean repeated = field.getRule()==FieldDescriptor.REPEATED_RULE;

        // Create the fields..
        p("// " + field.getRule() + " " + field.getType() + " " + field.getName() + " = " + field.getTag() + ";");
        if( repeated ) {
            // Create the field accessors
            p("public boolean has" + uname + "() {");
            indent();
            p("return bean().has" + uname + "();");
            unindent();
            p("}");
            p();
            p("public java.util.List<" + type + "> get" + uname + "List() {");
            indent();
            p("return bean().get" + uname + "List();");
            unindent();
            p("}");
            p();
            p("public int get" + uname + "Count() {");
            indent();
            p("return bean().get" + uname + "Count();");
            unindent();
            p("}");
            p();
            p("public " + type + " get" + uname + "(int index) {");
            indent();
            p("return bean().get" + uname + "(index);");
            unindent();
            p("}");
            p();
        } else {
            // Create the field accessors
            p("public boolean has" + uname + "() {");
            indent();
            p("return bean().has" + uname + "();");
            unindent();
            p("}");
            p();
            p("public " + type + " get" + uname + "() {");
            indent();
            p("return bean().get" + uname + "();");
            unindent();
            p("}");
            p();
        }
    }
    
    /**
     * @param field
     * @param className 
     */
    private void generateFieldGetterSignatures(FieldDescriptor field) {
        String uname = uCamel(field.getName());
        String type = field.getRule()==FieldDescriptor.REPEATED_RULE ? javaCollectionType(field):javaType(field);
        boolean repeated = field.getRule()==FieldDescriptor.REPEATED_RULE;

        // Create the fields..
        p("// " + field.getRule() + " " + field.getType() + " " + field.getName() + " = " + field.getTag() + ";");
        if( repeated ) {
            // Create the field accessors
            p("public boolean has" + uname + "();");
            p("public java.util.List<" + type + "> get" + uname + "List();");
            p("public int get" + uname + "Count();");
            p("public " + type + " get" + uname + "(int index);");
        } else {
            // Create the field accessors
            p("public boolean has" + uname + "();");
            p("public " + type + " get" + uname + "();");
        }
    }

  
    /**
     * @param field
     * @param className 
     */
    private void generateFieldAccessor(String beanClassName, FieldDescriptor field) {
        
        String lname = lCamel(field.getName());
        String uname = uCamel(field.getName());
        String type = field.getRule()==FieldDescriptor.REPEATED_RULE ? javaCollectionType(field):javaType(field);
        String typeDefault = javaTypeDefault(field);
        boolean primitive = field.getTypeDescriptor()==null || field.getTypeDescriptor().isEnum();
        boolean repeated = field.getRule()==FieldDescriptor.REPEATED_RULE;

        // Create the fields..
        p("// " + field.getRule() + " " + field.getType() + " " + field.getName() + " = " + field.getTag() + ";");
        
        if( repeated ) {
            p("private java.util.List<" + type + "> f_" + lname + ";");
            p();
            
            // Create the field accessors
            p("public boolean has" + uname + "() {");
            indent();
            p("return bean.f_" + lname + "!=null && !bean.f_" + lname + ".isEmpty();");
            unindent();
            p("}");
            p();

            p("public java.util.List<" + type + "> get" + uname + "List() {");
            indent();
            p("return bean.f_" + lname + ";");
            unindent();
            p("}");
            p();

            p("public java.util.List<" + type + "> create" + uname + "List() {");
            indent();
            p("copyCheck();");
            p("if( this.f_" + lname + " == null ) {");
            indent();
            p("this.f_" + lname + " = new java.util.ArrayList<" + type + ">();");
            unindent();
            p("}");
            p("return bean.f_" + lname + ";");
            unindent();
            p("}");
            p();
            
            p("public "+beanClassName+" set" + uname + "List(java.util.List<" + type + "> " + lname + ") {");
            indent();
          	p("copyCheck();");
            p("this.f_" + lname + " = " + lname + ";");
            p("return this;");
            unindent();
            p("}");
            p();
            
            p("public int get" + uname + "Count() {");
            indent();
            p("if( bean.f_" + lname + " == null ) {");
            indent();
            p("return 0;");
            unindent();
            p("}");
            p("return bean.f_" + lname + ".size();");
            unindent();
            p("}");
            p();
            
            p("public " + type + " get" + uname + "(int index) {");
            indent();
            p("if( bean.f_" + lname + " == null ) {");
            indent();
            p("return null;");
            unindent();
            p("}");
            p("return bean.f_" + lname + ".get(index);");
            unindent();
            p("}");
            p();
                            
            p("public "+beanClassName+" set" + uname + "(int index, " + type + " value) {");
            indent();
            p("this.create" + uname + "List().set(index, value);");
            p("return this;");
            unindent();
            p("}");
            p();
            
            p("public "+beanClassName+" add" + uname + "(" + type + " value) {");
            indent();
            p("this.create" + uname + "List().add(value);");
            p("return this;");
            unindent();
            p("}");
            p();
            
            p("public "+beanClassName+" addAll" + uname + "(java.lang.Iterable<? extends " + type + "> collection) {");
            indent();
            p("org.apache.activemq.protobuf.MessageBufferSupport.addAll(collection, this.create" + uname + "List());");
            p("return this;");
            unindent();
            p("}");
            p();

            p("public void clear" + uname + "() {");
            indent();
          	p("copyCheck();");
            p("this.f_" + lname + " = null;");
            unindent();
            p("}");
            p();

        } else {
            
            p("private " + type + " f_" + lname + " = "+typeDefault+";");
            if (primitive) {
                p("private boolean b_" + lname + ";");
            }
            p();
            
            // Create the field accessors
            p("public boolean has" + uname + "() {");
            indent();
            if (primitive) {
                p("return bean.b_" + lname + ";");
            } else {
                p("return bean.f_" + lname + "!=null;");
            }
            unindent();
            p("}");
            p();

            p("public " + type + " get" + uname + "() {");
            indent();
            p("return bean.f_" + lname + ";");
            unindent();
            p("}");
            p();

            p("public "+beanClassName+" set" + uname + "(" + type + " " + lname + ") {");
            indent();
          	p("copyCheck();");
            if (primitive) {
                if( auto_clear_optional_fields && field.isOptional() ) {
                    if( field.isStringType() && !"null".equals(typeDefault)) {
                        p("this.b_" + lname + " = ("+lname+" != "+typeDefault+");");
                    } else {
                        p("this.b_" + lname + " = ("+lname+" != "+typeDefault+");");
                    }
                } else {
                    p("this.b_" + lname + " = true;");
                }
            }
            p("this.f_" + lname + " = " + lname + ";");
            p("return this;");
            unindent();
            p("}");
            p();

            p("public void clear" + uname + "() {");
            indent();
          	p("copyCheck();");
            if (primitive) {
                p("this.b_" + lname + " = false;");
            }
            p("this.f_" + lname + " = " + typeDefault + ";");
            unindent();
            p("}");
            p();
        }

    }

    private String javaTypeDefault(FieldDescriptor field) {
        OptionDescriptor defaultOption = field.getOptions().get("default");
        if( defaultOption!=null ) {
            if( field.isStringType() ) {
                return asJavaString(defaultOption.getValue());
            } else if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
                return "new "+javaType(field)+"("+asJavaString(defaultOption.getValue())+")";
            } else if( field.isInteger32Type() ) {
                int v;
                if( field.getType() == FieldDescriptor.UINT32_TYPE ) {
                    v = TextFormat.parseUInt32(defaultOption.getValue());
                } else {
                    v = TextFormat.parseInt32(defaultOption.getValue());
                }
                return ""+v;
            } else if( field.isInteger64Type() ) {
                long v;
                if( field.getType() == FieldDescriptor.UINT64_TYPE ) {
                    v = TextFormat.parseUInt64(defaultOption.getValue());
                } else {
                    v = TextFormat.parseInt64(defaultOption.getValue());
                }
                return ""+v+"l";
            } else if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
                double v = Double.valueOf(defaultOption.getValue());
                return ""+v+"d";
            } else if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
                float v = Float.valueOf(defaultOption.getValue());
                return ""+v+"f";
            } else if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
                boolean v = Boolean.valueOf(defaultOption.getValue());
                return ""+v;
            } else if( field.getTypeDescriptor()!=null && field.getTypeDescriptor().isEnum() ) {
                return javaType(field)+"."+defaultOption.getValue();
            }
            return defaultOption.getValue();
        } else {
            if( field.isNumberType() ) {
                return "0";
            }
            if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
                return "false";
            }
            return "null";
        }
    }
        
    static final char HEX_TABLE[] = new char[]{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    
    private String asJavaString(String value) {
        StringBuilder sb = new StringBuilder(value.length()+2);
        sb.append("\"");
        for (int i = 0; i < value.length(); i++) {
            
          char b = value.charAt(i);
          switch (b) {
            // Java does not recognize \a or \v, apparently.
            case '\b': sb.append("\\b" ); break;
            case '\f': sb.append("\\f" ); break;
            case '\n': sb.append("\\n" ); break;
            case '\r': sb.append("\\r" ); break;
            case '\t': sb.append("\\t" ); break;
            case '\\': sb.append("\\\\"); break;
            case '\'': sb.append("\\\'"); break;
            case '"' : sb.append("\\\""); break;
            default:
              if (b >= 0x20 && b <'Z') {
                sb.append((char) b);
              } else {
                sb.append("\\u");
                sb.append(HEX_TABLE[(b >>> 12) & 0x0F] );
                sb.append(HEX_TABLE[(b >>> 8) & 0x0F] );
                sb.append(HEX_TABLE[(b >>> 4) & 0x0F] );
                sb.append(HEX_TABLE[b & 0x0F] );
              }
              break;
          }
          
        }
        sb.append("\"");
        return sb.toString();
    }

    private void generateEnum(EnumDescriptor ed) {
        String uname = uCamel(ed.getName());

        String staticOption = "static ";
        if( multipleFiles && ed.getParent()==null ) {
            staticOption="";
        }

        // TODO Auto-generated method stub
        p();
        p("public "+staticOption+"enum " +uname + " {");
        indent();
        
        
        p();
        int counter=0;
        for (EnumFieldDescriptor field : ed.getFields().values()) {
            boolean last = counter+1 == ed.getFields().size();
            p(field.getName()+"(\""+field.getName()+"\", "+field.getValue()+")"+(last?";":",")); 
            counter++;
        }
        p();
        p("private final String name;");
        p("private final int value;");
        p();
        p("private "+uname+"(String name, int value) {");
        p("   this.name = name;");
        p("   this.value = value;");
        p("}");
        p();
        p("public final int getNumber() {");
        p("   return value;");
        p("}");
        p();
        p("public final String toString() {");
        p("   return name;");
        p("}");
        p();
        p("public static "+uname+" valueOf(int value) {");
        p("   switch (value) {");
        
        // It's possible to define multiple ENUM fields with the same value.. 
        //   we only want to put the first one into the switch statement.
        HashSet<Integer> values = new HashSet<Integer>();
        for (EnumFieldDescriptor field : ed.getFields().values()) {
            if( !values.contains(field.getValue()) ) {
                p("   case "+field.getValue()+":");
                p("      return "+field.getName()+";");
                values.add(field.getValue());
            }
            
        }
        p("   default:");
        p("      return null;");
        p("   }");
        p("}");
        p();
        
        
        String createMessage = getOption(ed.getOptions(), "java_create_message", null);        
        if( "true".equals(createMessage) ) {
            
            p("public interface "+uname+"Creatable {");
            indent();
            p(""+uname+" to"+uname+"();");
            unindent();
            p("}");
            p();
            
            p("public "+uname+"Creatable createBean() {");
            indent();
            p("switch (this) {");
            indent();
            for (EnumFieldDescriptor field : ed.getFields().values()) {
                p("case "+field.getName()+":");
                String type = field.getAssociatedType().getName();
                p("   return new "+javaRelatedType(type, "Bean")+"();");
            }
            p("default:");
            p("   return null;");
            unindent();
            p("}");
            unindent();
            p("}");
            p();
            
            generateParseDelegate(ed, "parseUnframed", "org.apache.activemq.protobuf.Buffer", "org.apache.activemq.protobuf.InvalidProtocolBufferException");
            generateParseDelegate(ed, "parseFramed", "org.apache.activemq.protobuf.Buffer", "org.apache.activemq.protobuf.InvalidProtocolBufferException");
            generateParseDelegate(ed, "parseUnframed", "byte[]", "org.apache.activemq.protobuf.InvalidProtocolBufferException");
            generateParseDelegate(ed, "parseFramed", "byte[]", "org.apache.activemq.protobuf.InvalidProtocolBufferException");
            generateParseDelegate(ed, "parseFramed", "org.apache.activemq.protobuf.CodedInputStream", "org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException");
            generateParseDelegate(ed, "parseFramed", "java.io.InputStream", "org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException");
        }
        
        unindent();
        p("}");
        p();
    }

    private void generateParseDelegate(EnumDescriptor descriptor, String methodName, String inputType, String exceptions) {
        p("public org.apache.activemq.protobuf.MessageBuffer " + methodName + "(" + inputType + " data) throws " + exceptions + " {");
        indent();
        p("switch (this) {");
        indent();
        for (EnumFieldDescriptor field : descriptor.getFields().values()) {
            p("case "+field.getName()+":");
            String type = constantToUCamelCase(field.getName());
            p("   return "+javaRelatedType(type, "Buffer")+"."+methodName+"(data);");
        }
        p("default:");
        p("   return null;");
        unindent();
        p("}");
        unindent();
        p("}");
        p();
    }
    
    
    
    private String javaCollectionType(FieldDescriptor field) {
        if( field.isInteger32Type() ) {
            return "java.lang.Integer";
        }
        if( field.isInteger64Type() ) {
            return "java.lang.Long";
        }
        if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
            return "java.lang.Double";
        }
        if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
            return "java.lang.Float";
        }
        if( field.getType() == FieldDescriptor.STRING_TYPE ) {
            // TODO: support handling string fields as buffers.
//            String override = getOption(field.getOptions(), "java_override_type", null);
//            if( "AsciiBuffer".equals(override) ) {
//                return "org.apache.activemq.protobuf.AsciiBuffer";
//            } else if( "UTF8Buffer".equals(override) ) {
//                return "org.apache.activemq.protobuf.UTF8Buffer";
//            } else if( "Buffer".equals(override) ) {
//                return "org.apache.activemq.protobuf.Buffer";
//            } else {
                return "java.lang.String";
//            }
        }
        if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
            String override = getOption(field.getOptions(), "java_override_type", null);
            if( "AsciiBuffer".equals(override) ) {
                return "org.apache.activemq.protobuf.AsciiBuffer";
            } else if( "UTF8Buffer".equals(override) ) {
                return "org.apache.activemq.protobuf.UTF8Buffer";
            } else {
                return "org.apache.activemq.protobuf.Buffer";
            }
        }
        if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
            return "java.lang.Boolean";
        }
        
        TypeDescriptor descriptor = field.getTypeDescriptor();
        return javaType(descriptor);
    }

    private String javaType(FieldDescriptor field) {
        if( field.isInteger32Type() ) {
            return "int";
        }
        if( field.isInteger64Type() ) {
            return "long";
        }
        if( field.getType() == FieldDescriptor.DOUBLE_TYPE ) {
            return "double";
        }
        if( field.getType() == FieldDescriptor.FLOAT_TYPE ) {
            return "float";
        }
        if( field.getType() == FieldDescriptor.STRING_TYPE ) {
            // TODO: support handling string fields as buffers.
//            String override = getOption(field.getOptions(), "java_override_type", null);
//            if( "AsciiBuffer".equals(override) ) {
//                return "org.apache.activemq.protobuf.AsciiBuffer";
//            } else if( "UTF8Buffer".equals(override) ) {
//                return "org.apache.activemq.protobuf.UTF8Buffer";
//            } else if( "Buffer".equals(override) ) {
//                return "org.apache.activemq.protobuf.Buffer";
//            } else {
                return "java.lang.String";
//            }
        }
        if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
            String override = getOption(field.getOptions(), "java_override_type", null);
            if( "AsciiBuffer".equals(override) ) {
                return "org.apache.activemq.protobuf.AsciiBuffer";
            } else if( "UTF8Buffer".equals(override) ) {
                return "org.apache.activemq.protobuf.UTF8Buffer";
            } else {
                return "org.apache.activemq.protobuf.Buffer";
            }
        }
        if( field.getType() == FieldDescriptor.BOOL_TYPE ) {
            return "boolean";
        }
        
        TypeDescriptor descriptor = field.getTypeDescriptor();
        return javaType(descriptor);
    }

    private String javaType(TypeDescriptor descriptor) {
        ProtoDescriptor p = descriptor.getProtoDescriptor();
        if( p != proto ) {
            // Try to keep it short..
            String othePackage = javaPackage(p);
            if( equals(othePackage,javaPackage(proto) ) ) {
                return javaClassName(p)+"."+descriptor.getQName();
            }
            // Use the fully qualified class name.
            return othePackage+"."+javaClassName(p)+"."+descriptor.getQName();
        }
        return descriptor.getQName();
    }
    
    private String javaRelatedType(String type, String suffix) {
        int ix = type.lastIndexOf(".");
        if (ix == -1) {
            // type = Foo, result = Foo.FooBean
            return type+"."+type+suffix;
        }
        // type = Foo.Bar, result = Foo.Bar.BarBean
        return type+"."+type.substring(ix+1)+suffix;
    }

    private boolean equals(String o1, String o2) {
        if( o1==o2 )
            return true;
        if( o1==null || o2==null )
            return false;
        return o1.equals(o2);
    }

    private String javaClassName(ProtoDescriptor proto) {
        return getOption(proto.getOptions(), "java_outer_classname", uCamel(removeFileExtension(proto.getName())));
    }
    
    private boolean isMultipleFilesEnabled(ProtoDescriptor proto) {
        return "true".equals(getOption(proto.getOptions(), "java_multiple_files", "false"));
    }


    private String javaPackage(ProtoDescriptor proto) {
        String name = proto.getPackageName();
        if( name!=null ) {
            name = name.replace('-', '.');
            name = name.replace('/', '.');
        }
        return getOption(proto.getOptions(), "java_package", name);
    }


    // ----------------------------------------------------------------
    // Internal Helper methods
    // ----------------------------------------------------------------

    private void indent() {
        indent++;
    }

    private void unindent() {
        indent--;
    }

    private void p(String line) {
        // Indent...
        for (int i = 0; i < indent; i++) {
            w.print("   ");
        }
        // Then print.
        w.println(line);
    }

    private void p() {
        w.println();
    }

    private String getOption(Map<String, OptionDescriptor> options, String optionName, String defaultValue) {
        OptionDescriptor optionDescriptor = options.get(optionName);
        if (optionDescriptor == null) {
            return defaultValue;
        }
        return optionDescriptor.getValue();
    }
        
    static private String removeFileExtension(String name) {
        return name.replaceAll("\\..*", "");
    }

    static private String uCamel(String name) {
        boolean upNext=true;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if( Character.isJavaIdentifierPart(c) && Character.isLetterOrDigit(c)) {
                if( upNext ) {
                    c = Character.toUpperCase(c);
                    upNext=false;
                }
                sb.append(c);
            } else {
                upNext=true;
            }
        }
        return sb.toString();
    }

    static private String lCamel(String name) {
        if( name == null || name.length()<1 )
            return name;
        String uCamel = uCamel(name);
        return uCamel.substring(0,1).toLowerCase()+uCamel.substring(1);
    }
    
    
    private String constantToUCamelCase(String name) {
        boolean upNext=true;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if( Character.isJavaIdentifierPart(c) && Character.isLetterOrDigit(c)) {
                if( upNext ) {
                    c = Character.toUpperCase(c);
                    upNext=false;
                } else {
                    c = Character.toLowerCase(c);
                }
                sb.append(c);
            } else {
                upNext=true;
            }
        }
        return sb.toString();
    }


    private String constantCase(String name) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if( i!=0 && Character.isUpperCase(c) ) {
                sb.append("_");
            }
            sb.append(Character.toUpperCase(c));
        }
        return sb.toString();
    }

    public File getOut() {
        return out;
    }

    public void setOut(File outputDirectory) {
        this.out = outputDirectory;
    }

    public File[] getPath() {
        return path;
    }

    public void setPath(File[] path) {
        this.path = path;
    }
    
}
