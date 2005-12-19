package org.activeio.oneport.openorb;

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
    public org.activeio.oneport.openorb.TestIIOPServer value;

    /**
     * Default constructor
     */
    public TestIIOPServerHolder()
    { }

    /**
     * Constructor with value initialisation
     * @param initial the initial value
     */
    public TestIIOPServerHolder(org.activeio.oneport.openorb.TestIIOPServer initial)
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
