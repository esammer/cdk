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
package com.cloudera.data.lcr;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public class LCROutputStream extends OutputStream {

  private static final Logger logger = LoggerFactory.getLogger(LCROutputStream.class);

  private File directory;
  private Supplier<String> fileNameSupplier;
  private int blockSize;
  private int fragments;
  private int groups;
  private int globalParityFragments;

  private List<OutputStream> fragmentStreams;

  public LCROutputStream(File directory, Supplier<String> fileNameSupplier, int blockSize, int fragments, int groups, int globalParityFragments) {
    this.directory = directory;
    this.fileNameSupplier = fileNameSupplier;
    this.fragmentStreams = Lists.newArrayList();
    this.blockSize = blockSize;
    this.fragments = fragments;
    this.groups = groups;
    this.globalParityFragments = globalParityFragments;
  }

  public void open() {
    logger.debug("Opening LCR stream:{}", this);

    for (int globalParityId = 0; globalParityId < globalParityFragments; globalParityId++) {
      File fragmentFileTmp = new File(directory, fileNameSupplier.get() + "-global-" + globalParityId + ".tmp");
      logger.debug("Opening global parity fragment:{} file:{}", globalParityId, fragmentFileTmp);
    }

    for (int groupId = 0; groupId < groups; groupId++) {

      File groupParityFileTmp = new File(directory, fileNameSupplier.get() + "-group-" + groupId + ".tmp");
      logger.debug("Opening group parity file:{}", groupParityFileTmp);

      for (int groupFragId = 0; groupFragId < (fragments / groups); groupFragId++) {
        logger.debug("Opening group:{} fragment:{}", groupId, groupFragId);

        File fragmentFileTmp = new File(directory, Joiner.on("-").join(fileNameSupplier.get(), "data", groupId, groupFragId) + ".tmp");
        logger.debug("Fragment file:{}", fragmentFileTmp);
      }
    }
  }

  @Override
  public void write(int b) throws IOException {
    // TODO: Unimplemented!
  }

  public void close() {
    logger.debug("Closing LCR stream:{}", this);

    for (int globalParityId = 0; globalParityId < globalParityFragments; globalParityId++) {
      logger.debug("Closing global parity fragment:{}", globalParityId);
    }

    for (int groupId = 0; groupId < groups; groupId++) {
      logger.debug("Closing group:{}", groupId);

      for (int groupFragId = 0; groupFragId < (fragments / groups); groupFragId++) {
        logger.debug("Closing group fragment:{}", groupFragId);
      }
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("directory", directory)
      .add("fileNameSupplier", fileNameSupplier)
      .add("blockSize", blockSize)
      .add("fragments", fragments)
      .add("groups", groups)
      .add("globalParityFragments", globalParityFragments)
      .add("fragmentStreams", fragmentStreams)
      .toString();
  }

}
