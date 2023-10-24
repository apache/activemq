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
<html>
<head>
<c:set var="pageTitle" value="Browse ${requestContext.queueBrowser.JMSDestination}"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>


<h2>Browse <c:out value="${requestContext.queueBrowser.JMSDestination}" /></h2>

<table id="messages" class="sortable autostripe">
<thead>
<tr>
<th>Message ID</th>
<th>Correlation ID</th>
<th>Persistence</th>
<th>Priority</th>
<th>Redelivered</th>
<th>Reply To</th>
<th>Timestamp</th>
<th>Type</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<%--    
<c:forEach items="${requestContext.queueBrowser.browser.enumeration}" var="row">
---%>
<jms:forEachMessage queueBrowser="${requestContext.queueBrowser.browser}" var="row">
<tr>
<td><a href="<c:url value='message.jsp?id=${row.JMSMessageID}' />" title="<c:out value='${row.JMSDestination}' />"><c:out value="${row.JMSMessageID}" /></a></td>
<td><c:out value="${row.JMSCorrelationID}" /></td>
<td><jms:persistent message="${row}"/></td>
<td><c:out value="${row.JMSPriority}" /></td>
<td><c:out value="${row.JMSRedelivered}" /></td>
<td><c:out value="${row.JMSReplyTo}" /></td>
<td><c:out value="${row.JMSTimestamp}" /></td>
<td><c:out value="${row.JMSType}" /></td>
<td>
    <a href="<c:url value='deleteDestination.action?destination=${row.JMSMessageID}&secret=${sessionScope["secret"]}'/>">Delete</a>
</td>
</tr>
</jms:forEachMessage>
<%--    
</c:forEach>
--%>
</tbody>
</table>

<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
