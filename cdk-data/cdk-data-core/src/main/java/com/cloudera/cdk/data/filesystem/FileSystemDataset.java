/**
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
package com.cloudera.cdk.data.filesystem;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetDescriptor;
import com.cloudera.cdk.data.DatasetException;
import com.cloudera.cdk.data.DatasetReader;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.FieldPartitioner;
import com.cloudera.cdk.data.Marker;
import com.cloudera.cdk.data.PartitionKey;
import com.cloudera.cdk.data.PartitionStrategy;
import com.cloudera.cdk.data.View;
import com.cloudera.cdk.data.impl.Accessor;
import com.cloudera.cdk.data.spi.AbstractDataset;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

class FileSystemDataset<E> extends AbstractDataset<E> {

  private static final Logger logger = LoggerFactory
    .getLogger(FileSystemDataset.class);

  private final FileSystem fileSystem;
  private final Path directory;
  private final String name;
  private final DatasetDescriptor descriptor;
  private final PartitionKey partitionKey;

  private final PartitionStrategy partitionStrategy;

  private final FileSystemView<E> unbounded;

  // reusable path converter, has no relevant state
  private final PathConversion convert;

  FileSystemDataset(FileSystem fileSystem, Path directory, String name,
    DatasetDescriptor descriptor, @Nullable PartitionKey partitionKey) {

    this.fileSystem = fileSystem;
    this.directory = directory;
    this.name = name;
    this.descriptor = descriptor;
    this.partitionKey = partitionKey;
    this.partitionStrategy =
      descriptor.isPartitioned() ? descriptor.getPartitionStrategy() : null;
    this.convert = new PathConversion();

    this.unbounded = new FileSystemView<E>(this);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public DatasetDescriptor getDescriptor() {
    return descriptor;
  }

  PartitionKey getPartitionKey() {
    return partitionKey;
  }

  FileSystem getFileSystem() {
    return fileSystem;
  }

  Path getDirectory() {
    return directory;
  }

  @Override
  public DatasetWriter<E> newWriter() {
    logger.debug("Getting writer to dataset:{}", this);

    return unbounded.newWriter();
  }

  @Override
  public DatasetReader<E> newReader() {
    logger.debug("Getting reader for dataset:{}", this);

    return unbounded.newReader();
  }

  @Override
  public Iterable<View<E>> getCoveringPartitions() {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get partitions on a non-partitioned dataset (name:%s)",
      name);

    return unbounded.getCoveringPartitions();
  }

  @Override
  public View<E> from(Marker start) {
    return unbounded.from(start);
  }

  @Override
  public View<E> fromAfter(Marker start) {
    return unbounded.fromAfter(start);
  }

  @Override
  public View<E> to(Marker end) {
    return unbounded.to(end);
  }

  @Override
  public View<E> toBefore(Marker end) {
    return unbounded.toBefore(end);
  }

  @Override
  public View<E> in(Marker partial) {
    return unbounded.in(partial);
  }

  @Override
  @Nullable
  public Dataset<E> getPartition(PartitionKey key, boolean allowCreate) {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get a partition on a non-partitioned dataset (name:%s)",
      name);

    logger.debug("Loading partition for key {}, allowCreate:{}", new Object[] {
      key, allowCreate });

    Path partitionDirectory = fileSystem.makeQualified(
        toDirectoryName(directory, key));

    try {
      if (!fileSystem.exists(partitionDirectory)) {
        if (allowCreate) {
          fileSystem.mkdirs(partitionDirectory);
        } else {
          return null;
        }
      }
    } catch (IOException e) {
      throw new DatasetException("Unable to locate or create dataset partition directory " + partitionDirectory, e);
    }

    int partitionDepth = key.getLength();
    PartitionStrategy subpartitionStrategy = Accessor.getDefault()
        .getSubpartitionStrategy(partitionStrategy, partitionDepth);

    return new FileSystemDataset.Builder()
        .name(name)
        .fileSystem(fileSystem)
        .descriptor(new DatasetDescriptor.Builder(descriptor)
            .location(partitionDirectory)
            .partitionStrategy(subpartitionStrategy)
            .build())
        .partitionKey(key)
        .build();
  }

  @Override
  public void dropPartition(PartitionKey key) {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to drop a partition on a non-partitioned dataset (name:%s)",
      name);
    Preconditions.checkArgument(key != null, "Partition key may not be null");

    logger.debug("Dropping partition with key:{} dataset:{}", key, name);

    Path partitionDirectory = toDirectoryName(directory, key);

    try {
      if (!fileSystem.delete(partitionDirectory, true)) {
        throw new DatasetException("Partition directory " + partitionDirectory
          + " for key " + key + " does not exist");
      }
    } catch (IOException e) {
      throw new DatasetException("Unable to locate or drop dataset partition directory " + partitionDirectory, e);
    }
  }

  @Override
  public Iterable<Dataset<E>> getPartitions() {
    Preconditions.checkState(descriptor.isPartitioned(),
      "Attempt to get partitions on a non-partitioned dataset (name:%s)",
      name);

    List<Dataset<E>> partitions = Lists.newArrayList();

    FileStatus[] fileStatuses;

    try {
      fileStatuses = fileSystem.listStatus(directory,
        PathFilters.notHidden());
    } catch (IOException e) {
      throw new DatasetException("Unable to list partition directory for directory " + directory, e);
    }

    for (FileStatus stat : fileStatuses) {
      Path p = fileSystem.makeQualified(stat.getPath());
      PartitionKey key = fromDirectoryName(p);
      PartitionStrategy subPartitionStrategy = Accessor.getDefault()
          .getSubpartitionStrategy(partitionStrategy, 1);
      Builder builder = new FileSystemDataset.Builder()
          .name(name)
          .fileSystem(fileSystem)
          .descriptor(new DatasetDescriptor.Builder(descriptor)
              .location(p)
              .partitionStrategy(subPartitionStrategy)
              .build())
          .partitionKey(key);

      partitions.add(builder.<E>build());
    }

    return partitions;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("name", name)
      .add("descriptor", descriptor).add("directory", directory)
      .add("dataDirectory", directory).add("partitionKey", partitionKey)
      .toString();
  }

  void accumulateDatafilePaths(Path directory, List<Path> paths)
    throws IOException {

    for (FileStatus status : fileSystem.listStatus(directory,
      PathFilters.notHidden())) {

      if (status.isDirectory()) {
        accumulateDatafilePaths(status.getPath(), paths);
      } else {
        paths.add(status.getPath());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private Path toDirectoryName(Path dir, PartitionKey key) {
    Path result = dir;
    for (int i = 0; i < key.getLength(); i++) {
      final FieldPartitioner fp = partitionStrategy.getFieldPartitioners().get(i);
      result = new Path(result, convert.dirnameForValue(fp, key.get(i)));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private PartitionKey fromDirectoryName(Path dir) {
    final FieldPartitioner fp = partitionStrategy.getFieldPartitioners().get(0);
    final List<Object> values = Lists.newArrayList();

    if (partitionKey != null) {
      values.addAll(partitionKey.getValues());
    }

    values.add(convert.valueForDirname(fp, dir.getName()));

    return Accessor.getDefault().newPartitionKey(values.toArray());
  }

  public static class Builder {

    private Configuration conf;
    private FileSystem fileSystem;
    private Path directory;
    private String name;
    private DatasetDescriptor descriptor;
    private PartitionKey partitionKey;

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    protected Builder fileSystem(FileSystem fs) {
      this.fileSystem = fs;
      return this;
    }

    public Builder configuration(Configuration conf) {
      this.conf = conf;
      return this;
    }

     public Builder descriptor(DatasetDescriptor descriptor) {
      Preconditions.checkArgument(descriptor.getLocation() != null,
          "Dataset location cannot be null");

      this.descriptor = descriptor;

      return this;
    }

    Builder partitionKey(@Nullable PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      return this;
    }

    public <E> FileSystemDataset<E> build() {
      Preconditions.checkState(this.name != null, "No dataset name defined");
      Preconditions.checkState(this.descriptor != null,
        "No dataset descriptor defined");
      Preconditions.checkState((conf != null) || (fileSystem != null),
          "Configuration or FileSystem must be set");

      this.directory = new Path(descriptor.getLocation());

      if (fileSystem == null) {
        try {
          this.fileSystem = directory.getFileSystem(conf);
        } catch (IOException ex) {
          throw new DatasetException("Cannot access FileSystem", ex);
        }
      }

      Path absoluteDirectory = fileSystem.makeQualified(directory);
      return new FileSystemDataset<E>(
          fileSystem, absoluteDirectory, name, descriptor, partitionKey);
    }
  }

}
