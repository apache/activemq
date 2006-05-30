/** 
 * 
 * Copyright 2004 Protique Ltd
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
 * 
 **/
package org.activemq.usecases.lars;

import java.lang.reflect.Constructor;

/**
 * @version $Revision: 1.1 $
 */
public class ServerLoader {

    public static void main(String[] args) {
        try {
            ClassLoader cl1 = new ClassLoader() {
            };
            ClassLoader cl2 = new ClassLoader() {
            };

            Class c1 = cl1.loadClass("org.activemq.usecases.lars.Server");
            Class c2 = cl2.loadClass("org.activemq.usecases.lars.Server");

            final Constructor i1 = c1.getConstructor(new Class[]{String.class});
            final Constructor i2 = c2.getConstructor(new Class[]{String.class});


            Thread t1 = new Thread() {
                public void run() {
                    try {
                        Server s1 = (Server) i1.newInstance(new Object[]{"A"});
                        s1.go();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            Thread t2 = new Thread() {
                public void run() {
                    try {
                        Server s2 = (Server) i2.newInstance(new Object[]{"B"});
                        s2.go();
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            t1.start();
            t2.start();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}
