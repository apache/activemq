---
title: "How does XBean compare to Spring 2"
slug: /how-does-xbean-compare-to-spring-2
---

Spring 2.0 has introduced the ability to handle custom XML languages inside the spring.xml. This is something we developed [XBean](http://geronimo.apache.org/xbean/) for way back in the Spring 1.x days.

The Spring hook requires component developers to write their own XML handling, processing DOMs and so forth. On various projects like ActiveMQ, Jencks, Jetty, ServiceMix, XFire - we didn't want to have to spend time hand-crafting parsing of XML code. So XBean is used to auto-create the parsing code along with an XSD and reference documentation for how to configure these services in custom XML languages.

So you can think of XBean as being like a JAXB2 tool for working inside Spring XML files

### Spring versions supported

So you can use XBean in Spring 1.x or Spring 2.x (for Spring 2.0 support you need XBean 2.6 or later along with the matching release of ActiveMQ).

If you are using Spring 1.x you use the XBean versions of the Spring ApplicationContext classes. In Spring 2.0 the XBean functionality works perfectly fine inside a regular spring.xml file. The only main difference between using the XBean ApplicationContext files and the Spring ones is that the Spring ones mandate validation, so your XML must include all the XSD information (which is handy for IDE completion anyway) whereas this is not mandatory in the XBean configuraiton files.

### Examples

Here's a regular Spring 2 XML file using ActiveMQ's XML inside it to configure a broker & destinations & a connection factory

[http://svn.apache.org/repos/asf/activemq/trunk/activemq-core/src/test/resources/spring-embedded-xbean.xml](http://svn.apache.org/repos/asf/activemq/trunk/activemq-core/src/test/resources/spring-embedded-xbean.xml)

Here's an example Spring XML for Spring 2 which configures ActiveMQ, ActiveMQ Resource Adapter and Jencks all using Spring 2 with XSD validation (and so completion)

[http://svn.codehaus.org/jencks/trunk/jencks/src/test/resources/org/jencks/xbean/xbean-validating.xml](http://svn.codehaus.org/jencks/trunk/jencks/src/test/resources/org/jencks/xbean/xbean-validating.xml)

