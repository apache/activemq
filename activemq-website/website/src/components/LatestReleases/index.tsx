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

import Link from '@docusaurus/Link';
import releases from '@site/src/data/generated/releases.json';
import {formatDate} from '@site/src/utils/formatDate';
import styles from './styles.module.css';

type ReleaseItem = {
  title: string;
  shortDescription: string;
  releaseDate: string;
  url: string;
};

// The newest releases of every component, generated from the release pages by
// scripts/generate-release-news.ts.
const releaseList: ReleaseItem[] = releases.slice(0, 3).map((release) => ({
  title: release.title,
  shortDescription: release.summary ?? '',
  releaseDate: formatDate(release.releaseDate),
  url: release.url,
}));

function ReleaseCard({title, shortDescription, releaseDate, url}: ReleaseItem) {
  return (
    <div className={styles.releaseCard}>
      <h6><Link to={url}>{title}</Link></h6>
      <p>{shortDescription} <Link to={url}>...more</Link></p>
      <p className={styles.releaseDate}>{releaseDate}</p>
    </div>
  );
}

export default function LatestReleases() {
  return (
    <section className={styles.latestReleases}>
      <div className="container">
        <div className={styles.releaseCards}>
          {releaseList.map((props, idx) => (
            <ReleaseCard key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
