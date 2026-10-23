/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

export interface NmsRelease {
  version: string;
  releaseDate: string;
  releaseNotes: string;
  shortDescription?: string;
  slug?: string;
}

// Releases are listed newest first.
export const nmsApiReleases: NmsRelease[] = [
  {
    version: '2.2.0',
    releaseDate: '2025-07-05',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12351670',
    shortDescription:
      'Adds configurable acknowledgment handling for expired messages via IRedeliveryPolicy',
  },
  {
    version: '2.1.0',
    releaseDate: '2023-09-24',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12353620',
    shortDescription: 'Asynchronous consumer API',
  },
  {
    version: '2.0.0',
    releaseDate: '2021-07-03',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12349543',
    shortDescription: 'This release brings NMS 2.0 support.',
  },
  {
    version: '1.8.1',
    releaseDate: '2022-05-29',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12349546',
    shortDescription: 'Maintenance release.',
  },
  {
    version: '1.8.0',
    releaseDate: '2019-07-01',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12332992',
    shortDescription:
      'This release of the NMS API adds .net standard 2.0 support, and is the first release providing nuget packages.',
  },
];

export const nmsAmqpReleases: NmsRelease[] = [
  {
    version: '2.4.0',
    releaseDate: '2025-08-24',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12356027',
    shortDescription:
      'New acknowledgment customization for expired messages, client-side redelivery delay, and security fix for binary serialization.',
    slug: 'nms-amqp-02-04-00-release',
  },
  {
    version: '2.3.0',
    releaseDate: '2025-05-14',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12355896',
    shortDescription:
      'Support for asynchronous message consumption, configurable hostname/vhost in the AMQP Open frame, and includes a fix for NMSContext event handling.',
    slug: 'nms-amqp-02-03-00-release',
  },
  {
    version: '2.2.0',
    releaseDate: '2023-05-14',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12353210',
    shortDescription:
      'Message acknowledgement enhancements with support for multiple AckTypes.',
    slug: 'nms-amqp-02-02-00-release',
  },
  {
    version: '2.1.0',
    releaseDate: '2023-03-21',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12353001',
    shortDescription:
      'Security enhancement for binary serialization by adding allow/deny list of types.',
    slug: 'nms-amqp-02-01-00-release',
  },
  {
    version: '2.0.0',
    releaseDate: '2021-10-27',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12349544',
    shortDescription: 'NMS 2.0 implementation.',
    slug: 'nms-amqp-02-00-00-release',
  },
  {
    version: '1.8.2',
    releaseDate: '2021-08-16',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12350613',
    shortDescription: 'Bug fix release around send timeout.',
    slug: 'nms-amqp-01-08-02-release',
  },
  {
    version: '1.8.1',
    releaseDate: '2021-04-04',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12346504',
    slug: 'nms-amqp-01-08-01-release',
  },
  {
    version: '1.8.0',
    releaseDate: '2019-11-04',
    releaseNotes:
      'https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12311201&version=12332992',
    slug: 'nms-amqp-01-08-00-release',
  },
];

// Current release stream "x.y." prefixes, mirroring src/_data/current_releases.yml
// of the original Jekyll site.
export const nmsApiCurrentPrefixes: string[] = ['2.2.', '1.8.'];
export const nmsAmqpCurrentPrefixes: string[] = ['2.4.'];
