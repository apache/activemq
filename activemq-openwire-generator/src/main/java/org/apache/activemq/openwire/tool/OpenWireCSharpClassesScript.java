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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;

import org.codehaus.jam.JProperty;

/**
 *
 * @version $Revision: 383749 $
 */
public abstract class OpenWireCSharpClassesScript extends OpenWireClassesScript {

    public Object run() {
        filePostFix = ".cs";
        if (destDir == null) {
            destDir = new File("../activemq-dotnet/src/main/csharp/ActiveMQ/Commands");
        }
        
        return super.run();
    }
    
    public String makeHashCodeBody() throws Exception {
        if (simpleName.endsWith("Id")) {
            StringWriter buffer = new StringWriter();
            PrintWriter out = new PrintWriter(buffer);
            out.println("            int answer = 0;");
            Iterator iter = getProperties().iterator();
            while (iter.hasNext()) {
                JProperty property = (JProperty) iter.next();
                out.println("            answer = (answer * 37) + HashCode(" + property.getSimpleName() + ");");
            }
            out.println("            return answer;");
            return buffer.toString();
        }
        return null;
    }

    public String makeEqualsBody() throws Exception {
        if (simpleName.endsWith("Id")) {
            StringWriter buffer = new StringWriter();
            PrintWriter out = new PrintWriter(buffer);
            
            Iterator iter = getProperties().iterator();
            while (iter.hasNext()) {
                JProperty property = (JProperty) iter.next();
                String name = property.getSimpleName();
                out.println("            if (! Equals(this." + name + ", that." + name + ")) return false;");
            }
            out.println("            return true;");
            return buffer.toString();
        }
        return null;
    }
    
    public String makeToStringBody() throws Exception {
            StringWriter buffer = new StringWriter();
            PrintWriter out = new PrintWriter(buffer);
            out.println("            return GetType().Name + \"[\"");
            Iterator iter = getProperties().iterator();
            while (iter.hasNext()) {
                JProperty property = (JProperty) iter.next();
                String name = property.getSimpleName();
                out.println("                + \" " + name + "=\" + " + name);
            }
            out.println("                + \" ]\";");
            return buffer.toString();
    }


}
