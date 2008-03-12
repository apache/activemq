<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
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
    <style type="text/css" media="screen">
        @import url(<c:url value="/styles/sorttable.css"/>);
        @import url(<c:url value="/styles/type-settings.css"/>);
        @import url(<c:url value="/styles/site.css"/>);
    </style>
    <c:if test="${!disableJavaScript}">
	    <script type='text/javascript' src='<c:url value="/js/common.js"/>'></script>
	    <script type='text/javascript' src='<c:url value="/js/css.js"/>'></script>
	    <script type='text/javascript' src='<c:url value="/js/standardista-table-sorting.js"/>'></script>
    </c:if>

	<decorator:head />
</head>

<body>
                                                

<div class="white_box">
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
                               href="http://activemq.apache.org/"
                               title="The most popular and powerful open source Message Broker">ActiveMQ</a>
                            <a style="float:right; width:210px;display:block;text-indent:-5000px;text-decoration:none;line-height:60px; margin-top:15px; margin-right:10px;"
                               href="http://www.apache.org/" title="The Apache Software Foundation">ASF</a>
                        </div>
                    </div>


                    <div class="top_red_bar">
                        <div id="site-breadcrumbs">
                            <a href="<c:url value='/index.jsp'/>" title="Home">Home</a>
                            &#124;
                            <a href="<c:url value='/queues.jsp'/>" title="Queues">Queues</a>
                            &#124;
                            <a href="<c:url value='/topics.jsp'/>" title="Topics">Topics</a>
                            &#124;
                            <a href="<c:url value='/subscribers.jsp'/>" title="Subscribers">Subscribers</a>
                            &#124;
                            <a href="<c:url value='/send.jsp'/>"
                               title="Send">Send</a>
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

                                    <div class="navigation">
                                        <div class="navigation_top">
                                            <div class="navigation_bottom">
                                                <H3>Queue Views</H3>

                                                <ul class="alternate" type="square">



                                                    <li><a href="queueGraph.jsp" title="View the queue depths as a graph">Graph</a></li>
                                                    <li><a href="xml/queues.jsp" title="View the queues as XML">XML</a></li>
                                                </ul>
                                                <H3>Useful Links</H3>

                                                <ul class="alternate" type="square">
                                                    <li><a href="http://activemq.apache.org/"
                                                           title="The most popular and powerful open source Message Broker">Documentation</a></li>
                                                    <li><a href="http://activemq.apache.org/faq.html">FAQ</a></li>
                                                    <li><a href="http://activemq.apache.org/download.html">Downloads</a>
                                                    </li>
                                                    <li><a href="http://activemq.apache.org/discussion-forums.html">Forums</a>
                                                    </li>
                                                </ul>
                                            </div>
                                        </div>
                                    </div>
                                </td>
                            </tr>
                        </tbody>
                    </table>


                    <div class="bottom_red_bar"></div>
                </div>
            </div>
        </div>
    </div>
    <div class="black_box">
        <div class="footer">
            <div class="footer_l">
                <div class="footer_r">
                    <div>
                        Copyright 2005-2007 The Apache Software Foundation.

                        (<a href="?printable=true">printable version</a>)
                    </div>
                </div>
            </div>
        </div>
    </div>
</div>
<div class="design_attribution"><a href="http://hiramchirino.com/">Graphic Design By Hiram</a></div>

</body>
</html>
	
