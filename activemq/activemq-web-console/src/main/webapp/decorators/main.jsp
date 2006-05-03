<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
	<title><decorator:title default="ActiveMQ Console" /></title>
    <style type="text/css" media="screen">
        @import url(<c:url value="/styles/tools.css"/>);
        @import url(<c:url value="/styles/forms.css"/>);
        @import url(<c:url value="/styles/layout-navtop-subright.css"/>);
        @import url(<c:url value="/styles/layout.css"/>);
        @import url(<c:url value="/styles/tools.css"/>);
        @import url(<c:url value="/styles/sorttable.css"/>);
        @import url(<c:url value="/styles/deliciouslyblue.css"/>);
    </style>
	    <c:if test="${!disableJavaScript}">
	    <script type='text/javascript' src='<c:url value="/js/common.js"/>'/>
	    <script type='text/javascript' src='<c:url value="/js/css.js"/>'/>
	    <script type='text/javascript' src='<c:url value="/js/standardista-table-sorting.js"/>'/>
    </c:if>
        
	<decorator:head />
</head>

<body id="page-home">

<div id="page">
    
<div id="header" class="clearfix">
<!--    
<form name="form1" id="form1" method="post" action="">
<input type="text" name="textfield" value="Search For..." />
<input class=" button" type="submit" name="Submit" value="GO" />
</form>
-->

<h1>ActiveMQ Console</h1>
</div>

<div id="content" class="clearfix">
<div id="main">

<decorator:body />

</div>
            
<div id="nav">
  <ul>
    <li><a href="<c:url value='/index.jsp'/>" title="Home<"><span>Home</span></a></li>
    <li><a href="<c:url value='/queues.jsp'/>" title="Queues"><span>Queues</span></a></li>
    <li><a href="<c:url value='/topics.jsp'/>" title="Topics"><span>Topics</span></a></li>
    <li><a href="<c:url value='/subscribers.jsp'/>" title="Subscribers"><span>Subscribers</span></a></li>
    <li><a href="<c:url value='/send.jsp'/>" title="Send"><span>Send</span></a></li>
  </ul>
</div>

<div id="footer">
<p>
Copyright 2005-2006 The Apache Software Foundation
</p>
<p><small>(<a href="?printable=true">printable version</a>)</small></p>
</div>

</div>

</body>
</html>
	