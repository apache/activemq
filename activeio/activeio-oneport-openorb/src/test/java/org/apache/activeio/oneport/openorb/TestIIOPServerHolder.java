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
package org.apache.activeio.oneport.openorb;

/**
 * Holder class for : TestIIOPServer
 * 
 * @author OpenORB Compiler
 */
final public class TestIIOPServerHolder
        implements org.omg.CORBA.portable.Streamable
{
    /**
     * Internal TestIIOPServer value
     */
    public org.apache.activeio.oneport.openorb.TestIIOPServer value;

    /**
     * Default constructor
     */
    public TestIIOPServerHolder()
    { }

    /**
     * Constructor with value initialisation
     * @param initial the initial value
     */
    public TestIIOPServerHolder(org.apache.activeio.oneport.openorb.TestIIOPServer initial)
    {
        value = initial;
    }

    /**
     * Read TestIIOPServer from a marshalled stream
     * @param istream the input stream
     */
    public void _read(org.omg.CORBA.portable.InputStream istream)
    {
        value = TestIIOPServerHelper.read(istream);
    }

    /**
     * Write TestIIOPServer into a marshalled stream
     * @param ostream the output stream
     */
    public void _write(org.omg.CORBA.portable.OutputStream ostream)
    {
        TestIIOPServerHelper.write(ostream,value);
    }

    /**
     * Return the TestIIOPServer TypeCode
     * @return a TypeCode
     */
    public org.omg.CORBA.TypeCode _type()
    {
        return TestIIOPServerHelper.type();
    }

}
