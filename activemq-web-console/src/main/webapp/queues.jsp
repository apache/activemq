<html>
<head>
<title>Queues</title>
</head>
<body>

<div>
<form action="createDestination.action" method="get">
    <input type="hidden" name="JMSDestinationType" value="queue"/>

    <label name="destination">Queue Name</label>
    <input type="text" name="JMSDestination" value=""/>

    <input type="submit" value="Create"/>
</form>
</div>

<h2>Queues</h2>

<a href="queueGraph.jsp">Queue Graph</a>

<table id="queues" class="sortable autostripe">
<thead>
<tr>
<th>Name</th>
<th>Number Of Pending Messages</th>
<th>Number Of Consumers</th>
<th>Messages Sent</th>
<th>Messages Received</th>
<th>Views</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.queues}" var="row">
<tr>
<td><a href="browse.jsp?JMSDestination=${row.name}">${row.name}</a></td>
<td>${row.queueSize}</td>
<td>${row.consumerCount}</td>
<td>${row.enqueueCount}</td>
<td>${row.dequeueCount}</td>
<td>
    <a href="browse.jsp?JMSDestination=${row.name}">Browse</a>
<%--    
    <a href="graph.jsp?JMSDestination=${row.name}">Graph</a>
--%>    
    <a href="queueBrowse/${row.name}?view=rss&feedType=atom_1.0" title="Atom 1.0"><img src="images/atombadge.png"/></a>
    <a href="queueBrowse/${row.name}?view=rss&ffeedType=rss_2.0" title="RSS 2.0"><img src="images/rssbadge.gif"/></a>
</td>
<td>
    <a href="send.jsp?JMSDestination=${row.name}&JMSDestinationType=queue">Send To</a>
    <a href="purgeDestination.action?JMSDestination=${row.name}&JMSDestinationType=queue">Purge</a>
    <a href="deleteDestination.action?JMSDestination=${row.name}&JMSDestinationType=queue">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>


</body>
</html>
	