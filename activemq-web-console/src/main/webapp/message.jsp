<html>
<head>
<c:set var="row" value="${requestContext.messageQuery.message}"/>
<title>Message ${requestContext.messageQuery.id}</title>
</head>
<body>


<c:choose>
<c:when test="${empty row}">

<div>
No message could be found for ID ${requestContext.messageQuery.JMSMessageID}
</div>

</c:when>

<c:otherwise>

<table class="layout">
	<tr>
		<td class="layout"  valign="top">
			<table id="header" class="sortable autostripe">
				<thead>
					<tr>
						<th colspan="2">
						    Headers
						</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td class="label">Message ID</td>
						<td>${row.JMSMessageID}</td>
					</tr>
					<tr>
						<td class="label">Destination</td>
						<td>${row.JMSDestination}</td>
					</tr>
					<tr>
						<td class="label">Correlation ID</td>
						<td>${row.JMSCorrelationID}</td>
					</tr>
					<tr>
						<td class="label">Persistence</td>
						<td><jms:persistent message="${row}"/></td>
					</tr>
					<tr>
						<td class="label">Priority</td>
						<td>${row.JMSPriority}</td>
					</tr>
					<tr>
						<td class="label">Redelivered</td>
					    <td>${row.JMSRedelivered}</td>
					</tr>
					<tr>
						<td class="label">Reply To</td>
						<td>${row.JMSReplyTo}</td>
					</tr>
					<tr>
						<td class="label">Timestamp</td>
						<td>${row.JMSTimestamp}</td>
					</tr>
					<tr>
						<td class="label">Type</td>
						<td>${row.JMSType}</td>
					</tr>
				</tbody>
			</table>
        </td>

        <td  class="layout" valign="top">
			<table id="properties" class="sortable autostripe">
				<thead>
					<tr>
						<th colspan="2">
						    Properties
						</th>
					</tr>
				</thead>
				<tbody>
                   <form:forEachMapEntry items="${requestContext.messageQuery.propertiesMap}" var="row">
						<tr>
							<td class="label">${row.key}</td>
							<td>${row.value}</td>
						</tr>
						<tr>
					</form:forEachMapEntry>
				</tbody>
			</table>
		</td>
	</tr>
	<tr>
		<td class="layout" colspan="2">
			<table id="body" width="100%">
				<thead>
					<tr>
						<th>
						    Message Details
						</th>
					</tr>
				</thead>
				<tbody>
					<tr>
						<td>${requestContext.messageQuery.body}</td>
					</tr>
				</tbody>
			</table>
		</td>
	</tr>
</table>


</c:otherwise>
</c:choose>





</body>
</html>
	