<%@ taglib uri="http://www.opensymphony.com/sitemesh/decorator" prefix="decorator" %>

<html>
    <head>
        <title><decorator:title default="Welcome!" /></title>
        <decorator:head />
    </head>

    <body onload="window.print();">
        Printed on <%=new java.util.Date()%>.<br/>
        <hr noshade="noshade" size="1"/>
        <br/>
        <decorator:body />
    </body>
</html>