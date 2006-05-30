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
 * Interface definition: TestIIOPServer.
 * 
 * @author OpenORB Compiler
 */
public abstract class TestIIOPServerPOA extends org.omg.PortableServer.Servant
        implements TestIIOPServerOperations, org.omg.CORBA.portable.InvokeHandler
{
    public TestIIOPServer _this()
    {
        return TestIIOPServerHelper.narrow(_this_object());
    }

    public TestIIOPServer _this(org.omg.CORBA.ORB orb)
    {
        return TestIIOPServerHelper.narrow(_this_object(orb));
    }

    private static String [] _ids_list =
    {
        "IDL:org/activeio/oneport/TestIIOPServer:1.0"
    };

    public String[] _all_interfaces(org.omg.PortableServer.POA poa, byte [] objectId)
    {
        return _ids_list;
    }

    public final org.omg.CORBA.portable.OutputStream _invoke(final String opName,
            final org.omg.CORBA.portable.InputStream _is,
            final org.omg.CORBA.portable.ResponseHandler handler)
    {

        if (opName.equals("test")) {
                return _invoke_test(_is, handler);
        } else {
            throw new org.omg.CORBA.BAD_OPERATION(opName);
        }
    }

    // helper methods
    private org.omg.CORBA.portable.OutputStream _invoke_test(
            final org.omg.CORBA.portable.InputStream _is,
            final org.omg.CORBA.portable.ResponseHandler handler) {
        org.omg.CORBA.portable.OutputStream _output;

        test();

        _output = handler.createReply();

        return _output;
    }

}
