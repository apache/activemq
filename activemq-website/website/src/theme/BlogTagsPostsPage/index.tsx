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

import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import {
  PageMetadata,
  HtmlClassNameProvider,
  ThemeClassNames,
} from '@docusaurus/theme-common';
import SearchMetadata from '@theme/SearchMetadata';
import Unlisted from '@theme/ContentVisibility/Unlisted';
import type {Props} from '@theme/BlogTagsPostsPage';
import {NewsList, NewsPage} from '@site/src/components/News';

export default function BlogTagsPostsPage(props: Props) {
  const {tag, items} = props;
  const title = `${items.length} post${items.length === 1 ? '' : 's'} tagged with "${tag.label}"`;

  return (
    <HtmlClassNameProvider
      className={clsx(
        ThemeClassNames.wrapper.blogPages,
        ThemeClassNames.page.blogTagPostListPage,
      )}>
      <PageMetadata title={title} description={tag.description} />
      <SearchMetadata tag="blog_tags_posts" />
      <NewsPage
        title={tag.label}
        crumbs={[
          {label: 'Home', href: '/'},
          {label: 'News', href: '/news'},
          {label: tag.label},
        ]}>
        {tag.unlisted && <Unlisted />}
        <p className="margin-top--md">
          {title}. <Link to={tag.allTagsPath}>View All Tags</Link>
        </p>
        {tag.description && <p>{tag.description}</p>}
        <NewsList items={items} />
      </NewsPage>
    </HtmlClassNameProvider>
  );
}
