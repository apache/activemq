---
title: "Sample Camel Routes"
description: "Sample Apache Camel routes embedded in an ActiveMQ Classic broker: topic-to-queue bridging and content-based routing with JUEL and XPath filters and choice."
slug: /sample-camel-routes
---


### Example of a configuration that show how to use Camel routes with Juel and Xpath.
```
<beans>
    <!-- Allows us to use system properties as variables in this configuration file -->
    <bean class="org.springframework.beans.factory.config.PropertyPlaceholderConfigurer"/>

    <broker useJmx="true" persistent="false" xmlns="http://activemq.org/config/1.0"
            brokerName="localhost" dataDirectory="${activemq.base}/data">
    </broker>

    <camelContext id="camel" xmlns="http://activemq.apache.org/camel/schema/spring">
        <!-- simple bridge from a topic to queue -->
        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming">
            <to   uri="activemq:queue:queue.HelloWorld">
        </route>

        <!-- Dependencies: camel-juel-[camel-n].jar, juel-[juel-n]-impl.jar, juel-[juel-n].jar must be in the activemq lib directory -->
        <!-- 2 separate JUEL based filters -->
        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel1">
            <filter>
                <el>${in.headers.JMSType.equals('foo')}</el>
                <filter>
                    <el>${in.headers.myHeaderField.equals('takeMe')}</el>
                    <to   uri="activemq:queue:queue.HelloWorld.camel1.fooMsgs">
                </filter>
            </filter>
            <filter>
                <el>${in.headers.JMSType.equals('bar')}</el>
                <filter>
                    <el>${in.headers.myHeaderField.equals('takeMe')}</el>
                    <filter>
                        <el>${in.headers.myOtherHeaderField.equals('3')}</el>
                        <to   uri="activemq:queue:queue.HelloWorld.camel1.fooMsgs.special">
                    </filter>
                </filter>
            </filter>
        </route>

        <!-- route using 2 separate xpath based filters, assuming the message has an XML body -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel2">
            <filter>
                <xpath>$JMSType = 'foo' and $myCustomHeader='takeMe'</xpath>
                <to   uri="activemq:queue:queue.HelloWorld.camel2.fooMsgs">
            </filter>
            <filter>
                <xpath>$JMSType = 'bar' and $myCustomHeader='takeMe'</xpath>
                <to   uri="activemq:queue:queue.HelloWorld.camel2.barMsgs">
            </filter>
        </route>

        <!-- route using choice route based on xpath -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel3">
            <choice>
                <when>
                    <xpath>$JMSType = 'foo'</xpath>
                    <to   uri="activemq:queue:queue.HelloWorld.camel3.fooMsgs">
                </when>
                <otherwise>
                    <to   uri="activemq:queue:queue.HelloWorld.camel3.nonFooMsgs">
                </otherwise>
            </choice>
        </route>

        <!-- route using choice route based on JUEL -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel4">
            <choice>
                <when>
                    <el>${in.headers.JMSType.equals('foo')}</el>
                    <to uri="activemq:queue:queue.HelloWorld.camel4.fooMsgs">
                </when>
            </choice>
        </route>

        <!-- route using nested JUEL based filters -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel5">
            <filter>
                <el>${in.headers.JMSType.equals('foo')}</el>
                <filter>
                    <el>${in.headers.OperationRequest.equals('foo1')}</el>
                    <to uri="activemq:queue:queue.HelloWorld.camel5.foo1Msgs">
                </filter>
                <filter>
                    <el>${in.headers.OperationRequest.equals('foo2')}</el>
                    <to uri="activemq:queue:queue.HelloWorld.camel5.foo2Msgs">
                </filter>
            </filter>
        </route>

        <!-- route using nested JUEL and xpath filters, including a filter which examines the message content for an xml attribute -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel6">
            <filter>
                <el>${in.headers.JMSType.equals('foo')}</el>
                <filter>
                    <xpath>/rootXMLBodyNode/childXMLBodyNode[@myAttributeKey='myAttributeValue']</xpath>
                    <to uri="activemq:queue:queue.HelloWorld.camel6.fooMsgs">
                </filter>
            </filter>
        </route>

        <!-- route using nested JUEL and xpath filters, including a filter which examines the message content for an xml value -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel7">
            <filter>
                <el>${in.headers.JMSType.equals('Tideworks.GateVision.XmlCmd')}</el>
                <filter>
                    <xpath>/rootXMLBodyNode/childXMLBodyNode/elementKey = 'elementValue'</xpath>
                    <to uri="activemq:queue:queue.HelloWorld.camel7.fooMsgs">
                </filter>
            </filter>
        </route>

        <!-- route using nested JUEL and xpath filters, including a filter which examines the message content for an xml attribute -->

        <route>
            <from uri="activemq:topic:topic.HelloWorld.incoming.camel8">
            <filter>
                <el>${in.headers.JMSType.equals('foo')}</el>
                <filter>
                    <xpath>/rootXMLBodyNode/childXMLBodyNode[@myAttributeKey='myAttributeValue']/grandchildXMLBodyNode[@myAttributeKey='myAttributeValue']</xpath>
                    <to uri="activemq:queue:queue.HelloWorld.camel8.fooMsgs">
                </filter>
            </filter>
        </route>
    </camelContext>
</beans>
```
