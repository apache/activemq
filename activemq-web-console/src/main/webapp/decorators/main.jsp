<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
    <title>${requestContext.brokerQuery.brokerAdmin.brokerName} : <decorator:title default="ActiveMQ Console" /></title>
  
    <link rel="stylesheet" href="styles/bootstrap.min.css" />
   		<script type="text/javascript" src="js/jquery.min.js"></script>
        <script type="text/javascript" src="js/bootstrap.min.js"></script>
    
    
    <!-- If anyone uses sorttable in any other page you can put it back by adding: @import url('/admin/styles/sorttable.css'); -->
     <style type="text/css" media="screen">

        @import url('${pageContext.request.contextPath}/styles/type-settings.css');
        @import url('${pageContext.request.contextPath}/styles/site.css');
        @import url('${pageContext.request.contextPath}/styles/prettify.css');

footer-flot-bottom {
left: 0;
position: fixed;
text-align: center;
bottom: 0;
width: 100%;
}

    </style>
    <c:if test="${!disableJavaScript}">
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/common.js'></script>
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/css.js'></script>
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/standardista-table-sorting.js'></script>
        <script type='text/javascript' src='${pageContext.request.contextPath}/js/prettify.js'></script>
        <script>addEvent(window, 'load', prettyPrint)</script>
    </c:if>

    <decorator:head />
</head>

<body>

<div class=" ">
    <div class="header">
        <div class="header_l">
            <div class="header_r">
            </div>
        </div>
    </div>
    <div class="content">
        <div class="content_l">
            <div class="content_r">

                <div>

                    <!-- Banner -->
                    <div id="asf_logo">
                        <div id="activemq_logo">
                            <a style="float:left; width:280px;display:block;text-indent:-5000px;text-decoration:none;line-height:60px; margin-top:10px; margin-left:100px;"
                                href="http://activemq.apache.org/" title="The most popular and powerful open source Message Broker">ActiveMQ</a> &trade;
                            <a style="float:right; width:210px;display:block;text-indent:-5000px;text-decoration:none;line-height:60px; margin-top:15px; margin-right:10px;"
                                href="http://www.apache.org/" title="The Apache Software Foundation">ASF</a>
                        </div>
                    </div>
<!--
<ul class="nav nav-tabs">
  <li> <a href="/admin/index.jsp" title="Home">Home</a></li><li><a href="/admin/queues.jsp" title="Queues">Queues</a></li><li><a href="/admin/topics.jsp" title="Topics">Topics</a></li><li><a href="/admin/subscribers.jsp" title="Subscribers">Subscribers</a></li><li><a href="/admin/connections.jsp" title="Connections">Connections</a></li><li><a href="/admin/network.jsp" title="Network">Network</a></li><li><a href="/admin/scheduled.jsp" title="Scheduled">Scheduled</a></li><li><a href="/admin/send.jsp" title="Send">Send</a>
  </li><li></li></ul>
-->


                    <div id="navWrapper">
                        <div id="">
                         <ul class="nav nav-tabs">
                           <li>
                                <a href="<c:url value='/index.jsp'/>" title="Home">Home</a>
                           </li>
                           <li>
                                <a href="<c:url value='/queues.jsp'/>" title="Queues">Queues</a>
                           </li>
                            <li>
                                <a href="<c:url value='/topics.jsp'/>" title="Topics">Topics</a>
                            </li>
                            <li>
                                <a href="<c:url value='/subscribers.jsp'/>" title="Subscribers">Subscribers</a>
                            </li>
                            <li>
                                <a href="<c:url value='/connections.jsp'/>" title="Connections">Connections</a>
                            </li>
                            <li>
                                <a href="<c:url value='/network.jsp'/>" title="Network">Network</a>
                            </li>
                            <li>
                             <a href="<c:url value='/scheduled.jsp'/>" title="Scheduled">Scheduled</a>
                            </li>
                            <li>
                            <a href="<c:url value='/send.jsp'/>"
                               title="Send">Send</a>
                               </li>
                               </ul>
                        </div>
                        <div id="site-quicklinks"><P>
                            <a href="http://activemq.apache.org/support.html"
                               title="Get help and support using Apache ActiveMQ">Support</a></p>
                        </div>
                    </div>

                    <table border="0">
                        <tbody>
                            <tr>
                                <td valign="top" width="100%" style="overflow:hidden;">
                                    <div class="body-content">
                                        <decorator:body/>
                                    </div>
                                </td>
                                <td valign="top">
                                    <div class=" ">
                                        <div class=" ">
                                            <div class=" ">
                                                <H3>Queue Views</H3>
                                                <div class="list-group">
                                                <ul>



                                                    <li class="list-group-item"><a href="queueGraph.jsp" title="View the queue depths as a graph">Graph</a></li>
                                                    <li class="list-group-item"><a href="xml/queues.jsp" title="View the queues as XML">XML</a></li>
                                                </ul>
                                                <H3>Topic Views</H3>

                                                <ul  class="list-group">



                                                    <li class="list-group-item"><a href="xml/topics.jsp" title="View the topics as XML">XML</a></li>
                                                </ul>
                                                <H3>Subscribers Views</H3>

                                                <ul class="list-group">



                                                    <li  class="list-group-item"><a href="xml/subscribers.jsp" title="View the subscribers as XML">XML</a></li>
                                                </ul>
                                                <H3>Useful Links</H3>

                                                <ul  class="list-group">
                                                    <li  class="list-group-item"><a href="http://activemq.apache.org/"
                                                           title="The most popular and powerful open source Message Broker">Documentation</a></li>
                                                    <li  class="list-group-item"><a href="http://activemq.apache.org/faq.html">FAQ</a></li>
                                                    <li  class="list-group-item"><a href="http://activemq.apache.org/download.html">Downloads</a>
                                                    </li>
                                                    <li  class="list-group-item"><a href="http://activemq.apache.org/discussion-forums.html">Forums</a>
                                                    </li>
                                                </ul>
                                            </div>
                                        </div>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>


                 </div>
            </div>
        </div>
    </div>
    <div class="panel" id="footer-flot-bottom">
<div class="bottom_red_bar"></div>
        <div class="footer">
            <div class="footer_l">

                <div class="panel-heading">
                    <div>
                        Copyright 2005-2013 The Apache Software Foundation.

                        (<a href="?printable=true">printable version</a>)
                    </div>
                </div>
            </div>
        </div>
    </div>
<div class="panel-body design_attribution"><a href="http://hiramchirino.com/">Graphic Design By Hiram</a></div>
</div>

</body>
</html>

