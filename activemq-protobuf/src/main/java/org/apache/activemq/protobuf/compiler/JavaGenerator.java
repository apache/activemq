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
import static org.apache.activemq.protobuf.WireFormat.WIRETYPE_START_GROUP;
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

import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.protobuf.compiler.parser.ParseException;
import org.apache.activemq.protobuf.compiler.parser.ProtoParser;

public class JavaGenerator {

    private File out = new File(".");
    private File[] path = new File[]{new File(".")};

    private ProtoDescriptor proto;
    private String javaPackage;
    private String outerClassName;
    private PrintWriter w;
    private int indent;
    private ArrayList<String> errors = new ArrayList<String>();
    private boolean multipleFiles;
	private boolean deferredDecode;
    private boolean auto_clear_optional_fields;

    public static void main(String[] args) {
        
        JavaGenerator generator = new JavaGenerator();
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
		deferredDecode = Boolean.parseBoolean(getOption(proto.getOptions(), "deferred_decode", "false"));
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
        p();
        
        String staticOption = "static ";
        if( multipleFiles && m.getParent()==null ) {
            staticOption="";
        }
        
        String javaImplements = getOption(m.getOptions(), "java_implments", null);
        
        String implementsExpression = "";
        if( javaImplements!=null ) {
            implementsExpression = "implements "+javaImplements+" ";
        }
        
        String baseClass = "org.apache.activemq.protobuf.BaseMessage";
        if( deferredDecode ) {
            baseClass = "org.apache.activemq.protobuf.DeferredDecodeMessage";
        }
        if( m.getBaseType()!=null ) {
            baseClass = javaType(m.getBaseType())+"Base";
        }
        
        p(staticOption+"public final class " + className + " extends "+className+"Base<"+className+"> "+implementsExpression+"{");
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
        	if( isInBaseClass(m, field) ) {
        		continue;
        	}
            if( field.isGroup() ) {
                generateMessageBean(field.getGroup());
            }
        }

        
        generateMethodAssertInitialized(m, className);

        generateMethodClear(m);

        p("public "+className+" clone() {");
        p("   return new "+className+"().mergeFrom(this);");
        p("}");
        p();
        
        generateMethodMergeFromBean(m, className);

        generateMethodSerializedSize(m);
        
        generateMethodMergeFromStream(m, className);

        generateMethodWriteTo(m);

        generateMethodParseFrom(m, className);

        generateMethodToString(m);
        
        generateMethodVisitor(m);
                
        generateMethodType(m, className);
        
        generateMethodEquals(m, className);
                
        unindent();
        p("}");
        p();
        
        p(staticOption+"abstract class " + className + "Base<T> extends "+baseClass+"<T> {");
        p();
        indent();
        
        // Generate the field accessors..
        for (FieldDescriptor field : m.getFields().values()) {
            if( isInBaseClass(m, field) ) {
                continue;
            }
            generateFieldAccessor(field);
        }
        
        unindent();
        p("}");
        p();
    }

	private boolean isInBaseClass(MessageDescriptor m, FieldDescriptor field) {
		if( m.getBaseType() ==null )
			return false;
		return m.getBaseType().getFields().containsKey(field.getName());
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
    
    private void generateMethodParseFrom(MessageDescriptor m, String className) {
    	
    	String postMergeProcessing = ".checktInitialized()";
        if( deferredDecode ) {
        	postMergeProcessing="";
        }
        
        p("public static "+className+" parseUnframed(org.apache.activemq.protobuf.CodedInputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return new "+className+"().mergeUnframed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();

        p("public static "+className+" parseUnframed(org.apache.activemq.protobuf.Buffer data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return new "+className+"().mergeUnframed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();

        p("public static "+className+" parseUnframed(byte[] data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return new "+className+"().mergeUnframed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();
        
        p("public static "+className+" parseUnframed(java.io.InputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return new "+className+"().mergeUnframed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();
        
        p("public static "+className+" parseFramed(org.apache.activemq.protobuf.CodedInputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return new "+className+"().mergeFramed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();
        
        p("public static "+className+" parseFramed(org.apache.activemq.protobuf.Buffer data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return new "+className+"().mergeFramed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();

        p("public static "+className+" parseFramed(byte[] data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException {");
        indent();
        p("return new "+className+"().mergeFramed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();
        
        p("public static "+className+" parseFramed(java.io.InputStream data) throws org.apache.activemq.protobuf.InvalidProtocolBufferException, java.io.IOException {");
        indent();
        p("return new "+className+"().mergeFramed(data)"+postMergeProcessing+";");
        unindent();
        p("}");
        p();
    }

    private void generateMethodEquals(MessageDescriptor m, String className) {
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
        if( deferredDecode ) {
        	p("return toUnframedBuffer().equals(obj.toUnframedBuffer());");
        } else {        
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
        }
        unindent();
        p("}");
        p("");
        p("public int hashCode() {");
        indent();
        int hc = className.hashCode();
        if( deferredDecode ) {
            p("return "+hc+" ^ toUnframedBuffer().hashCode();");
        } else {
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
        }
        unindent();
        p("}");
        p("");
	}
    
    /**
     * @param m
     */
    private void generateMethodSerializedSize(MessageDescriptor m) {
        p("public int serializedSizeUnframed() {");
        indent();
        if( deferredDecode ) {
			p("if (encodedForm != null) {");
			indent();
            p("return encodedForm.length;");
			unindent();
			p("}");
        }
        p("if (memoizedSerializedSize != -1)");
        p("   return memoizedSerializedSize;");
        p();
        p("int size = 0;");
        for (FieldDescriptor field : m.getFields().values()) {
            
            String uname = uCamel(field.getName());
            String getter="get"+uname+"()";            
            String type = javaType(field);
            p("if (has"+uname+"()) {");
            indent();
            
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
                p("size += computeGroupSize("+field.getTag()+", "+getter+");");
            } else {
                p("size += computeMessageSize("+field.getTag()+", "+getter+");");
            }
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                unindent();
                p("}");
            }
            //TODO: finish this up.
            unindent();
            p("}");

        }
        // TODO: handle unknown fields
        // size += getUnknownFields().getSerializedSize();");
        p("memoizedSerializedSize = size;");
        p("return size;");
        unindent();
        p("}");
        p();
    }

    /**
     * @param m
     */
    private void generateMethodWriteTo(MessageDescriptor m) {
        p("public void writeUnframed(org.apache.activemq.protobuf.CodedOutputStream output) throws java.io.IOException {");
        indent();
        
        if( deferredDecode ) {
			p("if (encodedForm == null) {");
			indent();
            p("int size = serializedSizeUnframed();");
            p("encodedForm = output.getNextBuffer(size);");
            p("org.apache.activemq.protobuf.CodedOutputStream original=null;");
            p("if( encodedForm == null ) {");
            indent();
            p("encodedForm = new org.apache.activemq.protobuf.Buffer(new byte[size]);");
            p("original = output;");
            p("output = new org.apache.activemq.protobuf.CodedOutputStream(encodedForm);");
            unindent();
            p("}");
        }
        

        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            String getter="get"+uname+"()";            
            String type = javaType(field);
            p("if (has"+uname+"()) {");
            indent();
            
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                p("for ("+type+" i : get"+uname+"List()) {");
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
                p("writeGroup(output, "+field.getTag()+", "+getter+");");
            } else {
                p("writeMessage(output, "+field.getTag()+", "+getter+");");
            }
            
            if( field.getRule() == FieldDescriptor.REPEATED_RULE ) {
                unindent();
                p("}");
            }
            
            unindent();
            p("}");
        }
        
        if( deferredDecode ) {
            p("if( original !=null ) {");
            indent();
            p("output.checkNoSpaceLeft();");
            p("output = original;");
            p("output.writeRawBytes(encodedForm);");
            unindent();
            p("}");
            unindent();
            p("} else {");
            indent();
            p("output.writeRawBytes(encodedForm);");
            unindent();
            p("}");
        }

        unindent();
        p("}");
        p();        
    }

    /**
     * @param m
     * @param className
     */
    private void generateMethodMergeFromStream(MessageDescriptor m, String className) {
        p("public "+className+" mergeUnframed(org.apache.activemq.protobuf.CodedInputStream input) throws java.io.IOException {");
        indent();
		{        
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
						setter = "get" + uname + "List().add";
					}
					if (field.getType() == FieldDescriptor.STRING_TYPE) {
						p("case "
								+ makeTag(field.getTag(),
										WIRETYPE_LENGTH_DELIMITED) + ":");
						indent();
						p(setter + "(input.readString());");
					} else if (field.getType() == FieldDescriptor.BYTES_TYPE) {
						p("case "
								+ makeTag(field.getTag(),
										WIRETYPE_LENGTH_DELIMITED) + ":");
						indent();
						p(setter + "(input.readBytes());");
					} else if (field.getType() == FieldDescriptor.BOOL_TYPE) {
						p("case " + makeTag(field.getTag(), WIRETYPE_VARINT)
								+ ":");
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
						p("case "
								+ makeTag(field.getTag(), WIRETYPE_START_GROUP)
								+ ":");
						indent();
						String type = javaType(field);
						if (repeated) {
							p(setter + "(readGroup(input, " + field.getTag()
									+ ", new " + type + "()));");
						} else {
							p("if (has" + uname + "()) {");
							indent();
							p("readGroup(input, " + field.getTag() + ", get"
									+ uname + "());");
							unindent();
							p("} else {");
							indent();
							p(setter + "(readGroup(input, " + field.getTag()
									+ ",new " + type + "()));");
							unindent();
							p("}");
						}
						p("");
					} else {
						p("case "
								+ makeTag(field.getTag(),
										WIRETYPE_LENGTH_DELIMITED) + ":");
						indent();
						String type = javaType(field);
						if (repeated) {
							p(setter + "(new " + type
									+ "().mergeFramed(input));");
						} else {
							p("if (has" + uname + "()) {");
							indent();
							p("get" + uname + "().mergeFramed(input);");
							unindent();
							p("} else {");
							indent();
							p(setter + "(new " + type
									+ "().mergeFramed(input));");
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
        p("public "+className+" mergeFrom("+className+" other) {");
        indent();
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
                        p("get"+uname+"List().add(element.clone());");
                    unindent();
                    p("}");
                } else {
                    p("if (has"+uname+"()) {");
                    indent();
                    p("get"+uname+"().mergeFrom(other.get"+uname+"());");
                    unindent();
                    p("} else {");
                    indent();
                    p("set"+uname+"(other.get"+uname+"().clone());");
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
        p("super.clear();");
        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            p("clear" + uname + "();");
        }
        unindent();
        p("}");
        p();
    }

    private void generateMethodAssertInitialized(MessageDescriptor m, String className) {
        
        p("public java.util.ArrayList<String> missingFields() {");
        indent();
        p("java.util.ArrayList<String> missingFields = super.missingFields();");
        
        for (FieldDescriptor field : m.getFields().values()) {
            String uname = uCamel(field.getName());
            if( field.isRequired() ) {
                p("if(  !has" + uname + "() ) {");
                indent();
                p("missingFields.add(\""+field.getName()+"\");");
                unindent();
                p("}");
            }
        }
        
        if( !deferredDecode ) {
	        for (FieldDescriptor field : m.getFields().values()) {
	            if( field.getTypeDescriptor()!=null && !field.getTypeDescriptor().isEnum()) {
	                String uname = uCamel(field.getName());
	                p("if( has" + uname + "() ) {");
	                indent();
	                if( !field.isRepeated() ) {
	                    p("try {");
	                    indent();
	                    p("get" + uname + "().assertInitialized();");
	                    unindent();
	                    p("} catch (org.apache.activemq.protobuf.UninitializedMessageException e){");
	                    indent();
	                    p("missingFields.addAll(prefix(e.getMissingFields(),\""+field.getName()+".\"));");
	                    unindent();
	                    p("}");
	                } else {
	                    String type = javaCollectionType(field);
	                    p("java.util.List<"+type+"> l = get" + uname + "List();");
	                    p("for( int i=0; i < l.size(); i++ ) {");
	                    indent();
	                    p("try {");
	                    indent();
	                    p("l.get(i).assertInitialized();");
	                    unindent();
	                    p("} catch (org.apache.activemq.protobuf.UninitializedMessageException e){");
	                    indent();
	                    p("missingFields.addAll(prefix(e.getMissingFields(),\""+field.getName()+"[\"+i+\"]\"));");
	                    unindent();
	                    p("}");
	                    unindent();
	                    p("}");
	                }
	                unindent();
	                p("}");
	            }
	        }
        }
        p("return missingFields;");
        unindent();
        p("}");
        p();
    }

    private void generateMethodToString(MessageDescriptor m) {
        
        p("public String toString() {");
        indent();
        p("return toString(new java.lang.StringBuilder(), \"\").toString();");
        unindent();
        p("}");
        p();

        p("public java.lang.StringBuilder toString(java.lang.StringBuilder sb, String prefix) {");
        indent();
        
        if( deferredDecode ) {
        	p("load();");
        }        
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
    private void generateFieldAccessor(FieldDescriptor field) {
        
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
            if( deferredDecode ) {
            	p("load();");
            }        
            p("return this.f_" + lname + "!=null && !this.f_" + lname + ".isEmpty();");
            unindent();
            p("}");
            p();

            p("public java.util.List<" + type + "> get" + uname + "List() {");
            indent();
            if( deferredDecode ) {
            	p("load();");
            }        
            p("if( this.f_" + lname + " == null ) {");
            indent();
            p("this.f_" + lname + " = new java.util.ArrayList<" + type + ">();");
            unindent();
            p("}");
            p("return this.f_" + lname + ";");
            unindent();
            p("}");
            p();

            p("public T set" + uname + "List(java.util.List<" + type + "> " + lname + ") {");
            indent();
          	p("loadAndClear();");
            p("this.f_" + lname + " = " + lname + ";");
            p("return (T)this;");
            unindent();
            p("}");
            p();
            
            p("public int get" + uname + "Count() {");
            indent();
            if( deferredDecode ) {
            	p("load();");
            }        
            p("if( this.f_" + lname + " == null ) {");
            indent();
            p("return 0;");
            unindent();
            p("}");
            p("return this.f_" + lname + ".size();");
            unindent();
            p("}");
            p();
            
            p("public " + type + " get" + uname + "(int index) {");
            indent();
            if( deferredDecode ) {
            	p("load();");
            }        
            p("if( this.f_" + lname + " == null ) {");
            indent();
            p("return null;");
            unindent();
            p("}");
            p("return this.f_" + lname + ".get(index);");
            unindent();
            p("}");
            p();
                            
            p("public T set" + uname + "(int index, " + type + " value) {");
            indent();
          	p("loadAndClear();");
            p("get" + uname + "List().set(index, value);");
            p("return (T)this;");
            unindent();
            p("}");
            p();
            
            p("public T add" + uname + "(" + type + " value) {");
            indent();
          	p("loadAndClear();");
            p("get" + uname + "List().add(value);");
            p("return (T)this;");
            unindent();
            p("}");
            p();
            
            p("public T addAll" + uname + "(java.lang.Iterable<? extends " + type + "> collection) {");
            indent();
          	p("loadAndClear();");
            p("super.addAll(collection, get" + uname + "List());");
            p("return (T)this;");
            unindent();
            p("}");
            p();

            p("public void clear" + uname + "() {");
            indent();
          	p("loadAndClear();");
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
            if( deferredDecode ) {
            	p("load();");
            }        
            if (primitive) {
                p("return this.b_" + lname + ";");
            } else {
                p("return this.f_" + lname + "!=null;");
            }
            unindent();
            p("}");
            p();

            p("public " + type + " get" + uname + "() {");
            indent();
            if( deferredDecode ) {
            	p("load();");
            }        
            if( field.getTypeDescriptor()!=null && !field.getTypeDescriptor().isEnum()) {
                p("if( this.f_" + lname + " == null ) {");
                indent();
                p("this.f_" + lname + " = new " + type + "();");
                unindent();
                p("}");
            }
            p("return this.f_" + lname + ";");
            unindent();
            p("}");
            p();

            p("public T set" + uname + "(" + type + " " + lname + ") {");
            indent();
          	p("loadAndClear();");
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
            p("return (T)this;");
            unindent();
            p("}");
            p();

            p("public void clear" + uname + "() {");
            indent();
          	p("loadAndClear();");
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
                return "new org.apache.activemq.protobuf.Buffer("+asJavaString(defaultOption.getValue())+")";
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
            p("public org.apache.activemq.protobuf.Message createMessage() {");
            indent();
            p("switch (this) {");
            indent();
            for (EnumFieldDescriptor field : ed.getFields().values()) {
                p("case "+field.getName()+":");
                String type = constantToUCamelCase(field.getName());
                p("   return new "+type+"();");
            }
            p("default:");
            p("   return null;");
            unindent();
            p("}");
            unindent();
            p("}");
            p();
        }
        
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
            return "java.lang.String";
        }
        if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
            return "org.apache.activemq.protobuf.Buffer";
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
            return "java.lang.String";
        }
        if( field.getType() == FieldDescriptor.BYTES_TYPE ) {
            return "org.apache.activemq.protobuf.Buffer";
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
