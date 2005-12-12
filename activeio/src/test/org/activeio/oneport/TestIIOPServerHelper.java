package org.activeio.oneport;

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
    public static void insert(org.omg.CORBA.Any a, org.activeio.oneport.TestIIOPServer t)
    {
        a.insert_Object(t , type());
    }

    /**
     * Extract TestIIOPServer from an any
     *
     * @param a an any
     * @return the extracted TestIIOPServer value
     */
    public static org.activeio.oneport.TestIIOPServer extract( org.omg.CORBA.Any a )
    {
        if ( !a.type().equivalent( type() ) )
        {
            throw new org.omg.CORBA.MARSHAL();
        }
        try
        {
            return org.activeio.oneport.TestIIOPServerHelper.narrow( a.extract_Object() );
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
    public static org.activeio.oneport.TestIIOPServer read(org.omg.CORBA.portable.InputStream istream)
    {
        return(org.activeio.oneport.TestIIOPServer)istream.read_Object(org.activeio.oneport._TestIIOPServerStub.class);
    }

    /**
     * Write TestIIOPServer into a marshalled stream
     * @param ostream the output stream
     * @param value TestIIOPServer value
     */
    public static void write(org.omg.CORBA.portable.OutputStream ostream, org.activeio.oneport.TestIIOPServer value)
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
