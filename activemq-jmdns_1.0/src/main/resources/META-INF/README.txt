// %Z%%M%, %I%, %G%
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public
// License as published by the Free Software Foundation; either
// version 2.1 of the License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA


//This library is now licensed under the Apache License Version 2.0  Please
//see the file NOTICE.


Arthur van Hoff
avh@strangeberry.com

Rick Blair
rickblair@mac.com

** JmDNS

This is an implemenation of multi-cast DNS in Java. It currently
supports service discovery and service registration. It is fully
interoperable with Apple's Rendezvous.



** Requirements

jmdns has been tested using the JDK 1.3.1 and JDK 1.4.0
on the following platforms:

Windows 9x, XP, 2000
Linux   RedHat 7.3-9.0, Mandrake
Mac OSX



** Running jmdns from the Command Line

GUI browser:

  java -jar lib/jmdns.jar -browse

TTY browser for a particular service type:

  java -jar lib/jmdns.jar -bs _http._tcp local.

Register a service:

  java -jar lib/jmdns.jar -rs foobar _http._tcp local. 1234 path=index.html

List service types:

  java -jar lib/jmdns.jar -bt

To print debugging output specify -d as the first argument.



** Sample Code for Service Registration

    import javax.jmdns.*;

    JmDNS jmdns = new JmDNS();
    jmdns.registerService(
    	new ServiceInfo("_http._tcp.local.", "foo._http._tcp.local.", 1234, 0, 0, "path=index.html")
    );


** Sample code for Serivice Discovery

    import javax.jmdns.*;

    static class SampleListener implements ServiceListener
    {
	public void addService(JmDNS jmdns, String type, String name)
	{
	    System.out.println("ADD: " + jmdns.getServiceInfo(type, name));
	}
	public void removeService(JmDNS jmdns, String type, String name)
	{
	    System.out.println("REMOVE: " + name);
	}
	public void resolveService(JmDNS jmdns, String type, String name, ServiceInfo info)
	{
	    System.out.println("RESOLVED: " + info);
	}
    }

    JmDNS jmdns = new JmDNS();
    jmdns.addServiceListener("_http._tcp.local.", new SampleListener());


** Changes since October 2003

- Renamed package com.strangeberry.rendezvous to javax.jmdns
- fixed unicast queries
- fixed property handling
- use the hostname instead of the service name is address resolution
- Added Apache License.

--------------------------------------------------------------------
The activemq-jmdns_1.0 source is derived from http://repo1.maven.org/maven2/jmdns/jmdns/1.0/jmdns-1.0-sources.jar

Changes to apache activemq version:
- renamed package javax.jmdns to org.apache.activemq.jmdns
- removed classes with lgpl source headers, leaving only the org.apache.activemq.jmdns package.

