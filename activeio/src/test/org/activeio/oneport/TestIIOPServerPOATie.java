package org.activeio.oneport;

/**
 * Interface definition: TestIIOPServer.
 * 
 * @author OpenORB Compiler
 */
public class TestIIOPServerPOATie extends TestIIOPServerPOA
{

    //
    // Private reference to implementation object
    //
    private TestIIOPServerOperations _tie;

    //
    // Private reference to POA
    //
    private org.omg.PortableServer.POA _poa;

    /**
     * Constructor
     */
    public TestIIOPServerPOATie(TestIIOPServerOperations tieObject)
    {
        _tie = tieObject;
    }

    /**
     * Constructor
     */
    public TestIIOPServerPOATie(TestIIOPServerOperations tieObject, org.omg.PortableServer.POA poa)
    {
        _tie = tieObject;
        _poa = poa;
    }

    /**
     * Get the delegate
     */
    public TestIIOPServerOperations _delegate()
    {
        return _tie;
    }

    /**
     * Set the delegate
     */
    public void _delegate(TestIIOPServerOperations delegate_)
    {
        _tie = delegate_;
    }

    /**
     * _default_POA method
     */
    public org.omg.PortableServer.POA _default_POA()
    {
        if (_poa != null)
            return _poa;
        else
            return super._default_POA();
    }

    /**
     * Operation test
     */
    public void test()
    {
        _tie.test();
    }

}
