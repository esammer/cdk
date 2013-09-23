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
import com.cloudera.cdk.data.filesystem.FileSystemMetadataProvider;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;

public class TestDatasetRepositories {

  @Test
  public void testLocalRelative() throws URISyntaxException {
    DatasetRepository repository = DatasetRepositories.connect(new URI("dsr:file:target/dsr-repo-test"));

    // We only do the deeper implementation checks one per combination.
    Assert.assertNotNull("Received a repository", repository);
    Assert.assertTrue("Repo is a FileSystem repo", repository instanceof FileSystemDatasetRepository);
    Assert.assertTrue("FileSystem is a LocalFileSystem",
      ((FileSystemDatasetRepository) repository).getFileSystem() instanceof LocalFileSystem);
    Assert.assertTrue("Repo is using a FileSystemMetadataProvider",
      ((FileSystemDatasetRepository) repository).getMetadataProvider() instanceof FileSystemMetadataProvider);
    Assert.assertEquals(new Path("target/dsr-repo-test"), ((FileSystemDatasetRepository) repository).getRootDirectory());
  }

  @Test
  public void testLocalAbsolute() throws URISyntaxException {
    DatasetRepository repository = DatasetRepositories.connect(new URI("dsr:file:/tmp/dsr-repo-test"));

    Assert.assertEquals(new Path("/tmp/dsr-repo-test"), ((FileSystemDatasetRepository) repository).getRootDirectory());
  }

  @Test
  public void testHdfsAbsolute() throws URISyntaxException {
    DatasetRepository repository = DatasetRepositories.connect(new URI("dsr:hdfs://localhost:8020/tmp/dsr-repo-test"));

    // We only do the deeper implementation checks one per combination.
    Assert.assertNotNull("Received a repository", repository);
    Assert.assertTrue("Repo is a FileSystem repo", repository instanceof FileSystemDatasetRepository);
    Assert.assertTrue("FileSystem is a DistributedFileSystem",
      ((FileSystemDatasetRepository) repository).getFileSystem() instanceof DistributedFileSystem);
    Assert.assertTrue("Repo is using a FileSystemMetadataProvider",
      ((FileSystemDatasetRepository) repository).getMetadataProvider() instanceof FileSystemMetadataProvider);
    Assert.assertEquals(new Path("/tmp/dsr-repo-test"), ((FileSystemDatasetRepository) repository).getRootDirectory());
  }

}
