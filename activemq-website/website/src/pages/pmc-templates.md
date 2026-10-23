---
title: PMC Templates
---

# PMC Templates

The following templates are for use by the ActiveMQ PMC:

## Committer Discussion

```
To: private@activemq.apache.org
Subject: [DISCUSS] Invite <CANDIDATE> to become an ActiveMQ committer

Considering <CANDIDATE>'s contributions to the ActiveMQ project...<INSERT EVIDENCE HERE>

Based on this person's contributions above, I think we should invite <CANDIDATE> to become a committer.

Please remember that this is not a vote for this person to become a committer, at this point it is only a discussion.
```

## Committer Vote

```
To: private@activemq.apache.org
Subject: [VOTE] Offer <CANDIDATE> to become an ActiveMQ committer

After positive discussion about <CANDIDATE>'s contributions to the project, I would like to initiate a formal vote.

Considering <INSERT EVIDENCE HERE> as raised in the discussion thread...

This vote will follow the ASF process for committer votes and consensus approval (at least three binding +1 votes and no vetoes). The vote will be open for 72 hours.

[ ] +1 Accept <CANDIDATE> as a committer of Apache ActiveMQ
[ ] +0
[ ] -1 Do not accept at this time because...
```

## Committer Vote Result

```
To: private@activemq.apache.org
Subject: [RESULT][VOTE] Offer <CANDIDATE> to become an ActiveMQ committer.

This vote has been open for <X> days and is now closed with the following results:

* Number of +1s
* Number of 0s
* Number of -1s

The vote <passes|does not pass>.
```

## Committer Invitation

```
To: <INVITEE EMAIL>
Cc: private@activemq.apache.org
Subject: Invitation to become Apache ActiveMQ committer

Hello <INVITEE>,

The Apache ActiveMQ Project Management Committee (PMC) hereby offers you committer privileges to the project. These privileges are offered on the understanding that you'll use them reasonably and with common sense. We like to work on trust rather than unnecessary constraints.

Being a committer enables you to more easily make changes without needing to go through the patch submission process. Although you will be granted commit access to project repositories, you will be expected to adhere to project best practices when committing. The following URL some good resources regarding expectations and processes:

http://www.apache.org/dev/new-committers-guide.html

This is a personal invitation for you to accept or decline in private. Please reply to the private@activemq.apache.org address only.

If you accept the invitation, the next step is to register an Individual Contributor License Agreement (ICLA) with the Apache Software Foundation if you have not already. Details of the ICLA and the form can be found at the following URL:

http://www.apache.org/licenses/#clas

The form provides instructions for its completion and submittal to the Secretary of the ASF.

When you submit the completed ICLA, please request to notify the Apache ActiveMQ project upon official receipt. Once your ICLA has been acknowledged by the Secretary, you will need to choose a unique ASF ID. You can check to see if your preferred id is already taken by browsing the following list of ASF committers:

http://people.apache.org/committer-index.html

When your ICLA is recognized by the Secretary, you will receive a follow-up message with the next steps for establishing you as a committer.

We look forward to your response!
```

:::important
Once the prospective committer has accepted the invitation and the ICLA has been acknowledged (check the following URL to be sure it has been submitted: https://people.apache.org/unlistedclas.html), locate the vote result thread permalink from the private@activemq list can be found using: https://mail-search.apache.org/ or https://lists.apache.org/ (authentication required).

Upon locating the permalink, use the account request form to request the new ASF user id: https://id.apache.org/acreq/pmc-chairs. If the name isn't in the list be sure to click the link on the bottom right.
:::

## Committer Announcement

```
To: dev@activemq.apache.org
Subject: [ANNOUNCE] New Apache ActiveMQ Committer <INVITEE>

On behalf of the Apache ActiveMQ PMC, I am very pleased to announce that <INVITEE> has accepted an invitation to become a committer. We greatly appreciate all of <INVITEE>'s hard work and generous contributions to the project. We look forward to <her/his> continued involvement in the project.

Welcome <INVITEE> and congratulations!
```

## Committer Onboarding

```
To: private@activemq.apache.org, <NEW COMMITTER>
Subject: [committer]: What to do now that you're a committer?

<COMMITTER>, the ASF Infrastructure has set up your user account as the '<ASF USER ID>'.

To learn more about how the ASF works, please visit the following URL and read the items in the 'Committers and Contributors' section:

https://www.apache.org/dev/#committers

It is especially important to read through the New Committers Guide at the following URL:

https://www.apache.org/dev/new-committers-guide.html

Please ensure that you subscribed to the Apache ActiveMQ Developers mailing list dev@activemq.apache.org. We also recommend subscribing to the users@ and commits@ mailing lists at users@activemq.apache.org and commits@activemq.apache.org.

To push commits, ensure that you use HTTPS or SSH in your git remotes, or if you've been pulling from a mirror like Github, you'll need to add a remote such as:

$ git remote add apache https://git-wip-us.apache.org/repos/asf/activemq.git

If you have any questions please ask on the dev@activemq list.
```

## PMC Discussion

```
To: private@activemq.apache.org
Subject: [DISCUSS] Invite <CANDIDATE> to join ActiveMQ PMC

Considering <CANDIDATE>'s contributions to the ActiveMQ project as a committer...

<INSERT EVIDENCE HERE>

Based on this person's contributions above, I think we should invite <CANDIDATE> to become a PMC member.

Please remember that this is not a vote for this person to become a PMC member, at this point it is only a discussion.
```

## PMC Vote

```
To: private@activemq.apache.org
Subject: [VOTE] Offer Apache ActiveMQ PMC Membership to <CANDIDATE>

After positive discussion considering <CANDIDATE>'s contributions as a committer, I would like to initiate a formal vote.

<INSERT EVIDENCE HERE>

This vote will follow the ASF process for committer votes and consensus approval (at least three binding +1 votes and no vetoes)

The vote will be open for 72 hours.

[ ] +1 Accept <CANDIDATE> as a PMC member of Apache ActiveMQ
[ ] +0
[ ] -1 Do not accept at this time because...
```

## PMC Invite

```
To: <INVITEE EMAIL>
Cc: private@activemq.apache.org
Subject: Invitation to join the Apache ActiveMQ Project Management Committee

<INVITEE>,
In recognition of your continued contributions to the project, the Apache ActiveMQ PMC has voted to offer you membership to the PMC. I am writing on behalf of the PMC to give you the opportunity to either accept or reject this offer by replying to private@activemq.apache.org.

In making your decision, please see the following URL for more information on PMCs:

https://www.apache.org/dev/pmc.html

If you have questions before making a decision, please let us know.
```
