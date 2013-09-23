/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.cdk.data;

import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DatasetRepositories {

  private static final Logger logger = LoggerFactory.getLogger(DatasetRepositories.class);

  public static DatasetRepository connect(URI repositoryUri) {
    String scheme = repositoryUri.getScheme();
    String schemeSpecific = repositoryUri.getSchemeSpecificPart();

    Preconditions.checkArgument(scheme != null && scheme.equals("dsr"),
      "Invalid dataset repository URI:%s - scheme must be `dsr:`", repositoryUri);
    Preconditions.checkArgument(schemeSpecific != null,
      "Invalid dataset repository URI:%s - missing storage component", repositoryUri);

    DatasetRepository repo = null;
    URI uriInternal = null;

    try {
      uriInternal = new URI(schemeSpecific);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset repository URI:" + repositoryUri
        + " - storage component:" + schemeSpecific + " is malformed - " + e.getMessage());
    }

    Preconditions.checkArgument(uriInternal.getScheme() != null,
      "Invalid dataset repository URI:%s - storage component doesn't contain a valid scheme:%s",
      repositoryUri, schemeSpecific);

    if (uriInternal.getScheme().equals("file")) {
      Configuration conf = new Configuration();

      // TODO: Support non-2.0 versions.
      conf.set("fs.defaultFS", "file:///");

      Path basePath = null;

      // A URI's path can be null if it's relative. e.g. file:foo/bar.
      if (uriInternal.getPath() != null) {
        basePath = new Path(uriInternal.getPath());
      } else if (uriInternal.getSchemeSpecificPart() != null) {
        basePath = new Path(uriInternal.getSchemeSpecificPart());
      } else {
        throw new IllegalArgumentException("Invalid dataset repository URI:" + repositoryUri
          + " - storage component:" + schemeSpecific + " doesn't seem to have a path");
      }

      try {
        repo = new FileSystemDatasetRepository(FileSystem.get(conf), basePath);
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load Hadoop FileSystem implementation - " + e.getMessage(), e);
      }
    } else if (uriInternal.getScheme().equals("hdfs")) {
      Configuration conf = new Configuration();

      // TODO: Support non-2.0 versions.
      conf.set("fs.defaultFS", "hdfs://" + uriInternal.getAuthority() + "/");

      try {
        repo = new FileSystemDatasetRepository(FileSystem.get(conf), new Path(uriInternal.getPath()));
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to load Hadoop FileSystem implementation - " + e.getMessage(), e);
      }
    } else if (uriInternal.getScheme().equals("hive")) {
      throw new UnsupportedOperationException("Hive/HCatalog-based datasets are not yet supported. URI:" + repositoryUri);
    } else {
      throw new IllegalArgumentException("Invalid dataset repository URI:" + repositoryUri + " - unsupported storage method:" + schemeSpecific);
    }

    logger.debug("Connected to repository:{} using uri:{}", repo, repositoryUri);

    return repo;
  }
}