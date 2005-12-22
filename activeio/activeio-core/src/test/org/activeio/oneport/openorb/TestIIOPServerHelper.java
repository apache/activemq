/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activeio.oneport.openorb;


/** 
 * Helper class for : TestIIOPServer
 *  
 * @author OpenORB Compiler
 */ 
public class TestIIOPServerHelper
{
    /**
     * Insert TestIIOPServer into an any
     * @param a an any
     * @param t TestIIOPServer value
     */
    public static void insert(org.omg.CORBA.Any a, org.activeio.oneport.openorb.TestIIOPServer t)
    {
        a.insert_Object(t , type());
    }

    /**
     * Extract TestIIOPServer from an any
     *
     * @param a an any
     * @return the extracted TestIIOPServer value
     */
    public static org.activeio.oneport.openorb.TestIIOPServer extract( org.omg.CORBA.Any a )
    {
        if ( !a.type().equivalent( type() ) )
        {
            throw new org.omg.CORBA.MARSHAL();
        }
        try
        {
            return org.activeio.oneport.openorb.TestIIOPServerHelper.narrow( a.extract_Object() );
        }
        catch ( final org.omg.CORBA.BAD_PARAM e )
        {
            throw new org.omg.CORBA.MARSHAL(e.getMessage());
        }
    }

    //
    // Internal TypeCode value
    //
    private static org.omg.CORBA.TypeCode _tc = null;

    /**
     * Return the TestIIOPServer TypeCode
     * @return a TypeCode
     */
    public static org.omg.CORBA.TypeCode type()
    {
        if (_tc == null) {
            org.omg.CORBA.ORB orb = org.omg.CORBA.ORB.init();
            _tc = orb.create_interface_tc( id(), "TestIIOPServer" );
        }
        return _tc;
    }

    /**
     * Return the TestIIOPServer IDL ID
     * @return an ID
     */
    public static String id()
    {
        return _id;
    }

    private final static String _id = "IDL:org/activeio/oneport/TestIIOPServer:1.0";

    /**
     * Read TestIIOPServer from a marshalled stream
     * @param istream the input stream
     * @return the readed TestIIOPServer value
     */
    public static org.activeio.oneport.openorb.TestIIOPServer read(org.omg.CORBA.portable.InputStream istream)
    {
        return(org.activeio.oneport.openorb.TestIIOPServer)istream.read_Object(org.activeio.oneport.openorb._TestIIOPServerStub.class);
    }

    /**
     * Write TestIIOPServer into a marshalled stream
     * @param ostream the output stream
     * @param value TestIIOPServer value
     */
    public static void write(org.omg.CORBA.portable.OutputStream ostream, org.activeio.oneport.openorb.TestIIOPServer value)
    {
        ostream.write_Object((org.omg.CORBA.portable.ObjectImpl)value);
    }

    /**
     * Narrow CORBA::Object to TestIIOPServer
     * @param obj the CORBA Object
     * @return TestIIOPServer Object
     */
    public static TestIIOPServer narrow(org.omg.CORBA.Object obj)
    {
        if (obj == null)
            return null;
        if (obj instanceof TestIIOPServer)
            return (TestIIOPServer)obj;

        if (obj._is_a(id()))
        {
            _TestIIOPServerStub stub = new _TestIIOPServerStub();
            stub._set_delegate(((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate());
            return stub;
        }

        throw new org.omg.CORBA.BAD_PARAM();
    }

    /**
     * Unchecked Narrow CORBA::Object to TestIIOPServer
     * @param obj the CORBA Object
     * @return TestIIOPServer Object
     */
    public static TestIIOPServer unchecked_narrow(org.omg.CORBA.Object obj)
    {
        if (obj == null)
            return null;
        if (obj instanceof TestIIOPServer)
            return (TestIIOPServer)obj;

        _TestIIOPServerStub stub = new _TestIIOPServerStub();
        stub._set_delegate(((org.omg.CORBA.portable.ObjectImpl)obj)._get_delegate());
        return stub;

    }

}
