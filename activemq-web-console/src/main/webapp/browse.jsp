<html>
<head>
<title>Browse ${requestContext.queueBrowser.JMSDestination}</title>
</head>
<body>

<h2>Browse ${requestContext.queueBrowser.JMSDestination}</h2>

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
<jms:body message="${row}" var="body"/>
<td><a href="message.jsp?id=${row.JMSMessageID}" title="${body}">${row.JMSMessageID}</a></td>
<td>${row.JMSCorrelationID}</td>
<td><jms:persistent message="${row}"/></td>
<td>${row.JMSPriority}</td>
<td>${row.JMSRedelivered}</td>
<td>${row.JMSReplyTo}</td>
<td>${row.JMSTimestamp}</td>
<td>${row.JMSType}</td>
<td>
    <a href="deleteDestination.action?destination=${row.JMSMessageID}">Delete</a>
</td>
</tr>
</jms:forEachMessage>
<%--    
</c:forEach>
--%>
</tbody>
</table>


</body>
</html>
	