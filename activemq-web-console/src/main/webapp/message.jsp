<html>
<head>
<title>Message ${requestContext.messageBrowser.id}</title>
</head>
<body>

<table id="message" class="sortable autostripe">
<thead>
<tr>
<th>
    Message Details
</th>
</tr>
</thead>

<tbody>
<tr>
<td class="label">Message ID</td>
<td>${row.JMSMessageID</td>
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


</body>
</html>
	