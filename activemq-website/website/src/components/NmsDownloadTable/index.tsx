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

export type NmsProduct = 'api' | 'amqp';

interface NmsDownloadTableProps {
  product: NmsProduct;
  version: string;
  isCurrentRelease: boolean;
}

interface Artifact {
  description: string;
  fileName: string;
}

const DIST_NAME: Record<NmsProduct, string> = {
  api: 'apache-nms-api',
  amqp: 'apache-nms-amqp',
};

function artifacts(product: NmsProduct, version: string): Artifact[] {
  if (product === 'amqp') {
    return [
      {description: 'Apache.NMS.AMQP Source code', fileName: `Apache.NMS.AMQP-${version}-src.zip`},
      {description: 'Apache.NMS.AMQP Binary Assemblies', fileName: `Apache.NMS.AMQP-${version}-bin.zip`},
      {description: 'Apache.NMS.AMQP Nuget Package', fileName: `Apache.NMS.AMQP.${version}.nupkg`},
    ];
  }
  return [
    {description: 'Apache.NMS Source code', fileName: `Apache.NMS-${version}-src.zip`},
    {description: 'Apache.NMS Binary Assemblies', fileName: `Apache.NMS-${version}-bin.zip`},
    {description: 'Apache.NMS Nuget Package', fileName: `Apache.NMS.${version}.nupkg`},
    {description: 'Apache.NMS Documentation', fileName: `Apache.NMS-${version}-docs.zip`},
  ];
}

/**
 * Renders the download links of a single NMS release, replacing the
 * nms_api_download_links.md and nms_amqp_download_links.md includes of the
 * original Jekyll site. Current releases are served by the Apache mirrors,
 * older ones only from the archive.
 */
export default function NmsDownloadTable({
  product,
  version,
  isCurrentRelease,
}: NmsDownloadTableProps) {
  const dist = DIST_NAME[product];
  const mirrorBase = `https://www.apache.org/dyn/closer.lua?filename=/activemq/${dist}/${version}/`;
  const downloadsBase = `https://downloads.apache.org/activemq/${dist}/${version}/`;
  const archiveBase = `https://archive.apache.org/dist/activemq/${dist}/${version}/`;

  const downloadUrl = (fileName: string) =>
    isCurrentRelease ? `${mirrorBase}${fileName}&action=download` : `${archiveBase}${fileName}`;
  const verifyUrl = (fileName: string, extension: string) =>
    isCurrentRelease
      ? `${downloadsBase}${fileName}.${extension}`
      : `${archiveBase}${fileName}.${extension}`;

  return (
    <table>
      <thead>
        <tr>
          <th>Description</th>
          <th>Download Link</th>
          <th>SHA512</th>
          <th>Signature</th>
        </tr>
      </thead>
      <tbody>
        {artifacts(product, version).map((artifact) => (
          <tr key={artifact.fileName}>
            <td>{artifact.description}</td>
            <td>
              <a href={downloadUrl(artifact.fileName)}>{artifact.fileName}</a>
            </td>
            <td>
              <a href={verifyUrl(artifact.fileName, 'sha512')}>SHA512</a>
            </td>
            <td>
              <a href={verifyUrl(artifact.fileName, 'asc')}>PGP Signature</a>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
