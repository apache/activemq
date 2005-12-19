package org.activeio.oneport.openorb;


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

    private final static Class _opsClass = org.activeio.oneport.openorb.TestIIOPServerOperations.class;

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
                org.activeio.oneport.openorb.TestIIOPServerOperations _self = (org.activeio.oneport.openorb.TestIIOPServerOperations) _so.servant;
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
