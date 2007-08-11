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
import java.io.PrintWriter;

/**
 *
 * @version $Revision: 381410 $
 */
public class CppMarshallingHeadersGenerator extends JavaMarshallingGenerator {

	protected String targetDir="./src";

    public Object run() {
        filePostFix = getFilePostFix();
        if (destDir == null) {
            destDir = new File(targetDir+"/marshal");
        }
        return super.run();
    }	   
    
    protected String getFilePostFix() {
        return ".hpp";
    }
    
    
	protected void generateLicence(PrintWriter out) {
out.println("/*");
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

	protected void generateFile(PrintWriter out) throws Exception {
		generateLicence(out);
		
out.println("#ifndef "+className+"_hpp_");
out.println("#define "+className+"_hpp_");
out.println("");
out.println("#include <string>");
out.println("");
out.println("#include \"command/IDataStructure.hpp\"");
out.println("");
out.println("/* we could cut this down  - for now include all possible headers */");
out.println("#include \"command/BrokerId.hpp\"");
out.println("#include \"command/ConnectionId.hpp\"");
out.println("#include \"command/ConsumerId.hpp\"");
out.println("#include \"command/ProducerId.hpp\"");
out.println("#include \"command/SessionId.hpp\"");
out.println("");
out.println("#include \"io/BinaryReader.hpp\"");
out.println("#include \"io/BinaryWriter.hpp\"");
out.println("");
out.println("#include \"command/"+baseClass+".hpp\"");
out.println("#include \"util/ifr/p.hpp\"");
out.println("");
out.println("#include \"protocol/ProtocolFormat.hpp\"");
out.println("");
out.println("namespace apache");
out.println("{");
out.println("  namespace activemq");
out.println("  {");
out.println("    namespace client");
out.println("    {");
out.println("      namespace marshal");
out.println("      {");
out.println("        using namespace ifr ;");
out.println("        using namespace apache::activemq::client::command;");
out.println("        using namespace apache::activemq::client::io;");
out.println("        using namespace apache::activemq::client::protocol;");
out.println("");
out.println("/*");
out.println(" *");
out.println(" */");
out.println("class "+className+" : public "+baseClass+"");
out.println("{");
out.println("public:");
out.println("    "+className+"() ;");
out.println("    virtual ~"+className+"() ;");
out.println("");
out.println("    virtual IDataStructure* createCommand() ;");
out.println("    virtual char getDataStructureType() ;");
out.println("    ");
out.println("    virtual void unmarshal(ProtocolFormat& wireFormat, Object o, BinaryReader& dataIn, BooleanStream& bs) ;");
out.println("    virtual int marshal1(ProtocolFormat& wireFormat, Object& o, BooleanStream& bs) ;");
out.println("    virtual void marshal2(ProtocolFormat& wireFormat, Object& o, BinaryWriter& dataOut, BooleanStream& bs) ;");
out.println("} ;");
out.println("");
out.println("/* namespace */");
out.println("     }");
out.println("    }");
out.println("  }");
out.println("}");
out.println("#endif /*"+className+"_hpp_*/");
        }
 	
    public void generateFactory(PrintWriter out) {
		generateLicence(out);
out.println("");
out.println("// Marshalling code for Open Wire Format ");
out.println("//");
out.println("//");
out.println("// NOTE!: This file is autogenerated - do not modify!");
out.println("//        if you need to make a change, please see the Groovy scripts in the");
out.println("//        activemq-openwire module");
out.println("//");
out.println("");
out.println("#ifndef MarshallerFactory_hpp_");
out.println("#define MarshallerFactory_hpp_");
out.println("");
out.println("");
out.println("namespace apache");
out.println("{");
out.println("  namespace activemq");
out.println("  {");
out.println("    namespace client");
out.println("    {");
out.println("      namespace marshal");
out.println("      {");
out.println("        using namespace ifr ;");
out.println("        using namespace std ;");
out.println("        using namespace apache::activemq::client;");
out.println("        using namespace apache::activemq::client::command;");
out.println("        using namespace apache::activemq::client::io;");
out.println("");
out.println("/*");
out.println(" * ");
out.println(" */");
out.println("class MarshallerFactory");
out.println("{");
out.println("public:");
out.println("    MarshallerFactory() ;");
out.println("    virtual ~MarshallerFactory() ;");
out.println("");
out.println("	  virtual void configure(ProtocolFormat& format) ;");
out.println("} ;");
out.println("");
out.println("/* namespace */");
out.println("      }");
out.println("    }");
out.println("  }");
out.println("}");
out.println("");
out.println("#endif /*MarshallerFactory_hpp_*/");
out.println("");
    }

	public String getTargetDir() {
		return targetDir;
	}

	public void setTargetDir(String targetDir) {
		this.targetDir = targetDir;
	}
}
