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
public class _TestIIOPServerStub extends org.omg.CORBA.portable.ObjectImpl
        implements TestIIOPServer
{
    static final String[] _ids_list =
    {
        "IDL:org/activeio/oneport/TestIIOPServer:1.0"
    };

    public String[] _ids()
    {
     return _ids_list;
    }

    private final static Class _opsClass = org.apache.activeio.oneport.openorb.TestIIOPServerOperations.class;

    /**
     * Operation test
     */
    public void test()
    {
        while(true)
        {
            if (!this._is_local())
            {
                org.omg.CORBA.portable.InputStream _input = null;
                try
                {
                    org.omg.CORBA.portable.OutputStream _output = this._request("test",true);
                    _input = this._invoke(_output);
                    return;
                }
                catch(org.omg.CORBA.portable.RemarshalException _exception)
                {
                    continue;
                }
                catch(org.omg.CORBA.portable.ApplicationException _exception)
                {
                    String _exception_id = _exception.getId();
                    throw new org.omg.CORBA.UNKNOWN("Unexpected User Exception: "+ _exception_id);
                }
                finally
                {
                    this._releaseReply(_input);
                }
            }
            else
            {
                org.omg.CORBA.portable.ServantObject _so = _servant_preinvoke("test",_opsClass);
                if (_so == null)
                   continue;
                org.apache.activeio.oneport.openorb.TestIIOPServerOperations _self = (org.apache.activeio.oneport.openorb.TestIIOPServerOperations) _so.servant;
                try
                {
                    _self.test();
                    return;
                }
                finally
                {
                    _servant_postinvoke(_so);
                }
            }
        }
    }

}
