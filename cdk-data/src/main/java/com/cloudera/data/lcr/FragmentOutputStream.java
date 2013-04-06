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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;

public class FragmentOutputStream extends OutputStream {

  private static final Logger logger = LoggerFactory.getLogger(FragmentOutputStream.class);

  private OutputStream outputStream;
  private int blockSize;

  private byte[] buffer;
  private int currentBufferPos;
  private int currentParityA;
  private int currentParityB;

  public FragmentOutputStream(OutputStream outputStream, int blockSize) {
    super();

    this.blockSize = blockSize;
    this.outputStream = outputStream;

    this.buffer = new byte[blockSize];
    this.currentBufferPos = 0;
    this.currentParityA = 0;
    this.currentParityB = 0;
  }

  @Override
  public void write(int b) throws IOException {
    if (currentBufferPos >= blockSize) {
      flushBuffer();
    }

    currentParityA += b;
    currentParityB = currentParityB ^ b;
    buffer[currentBufferPos++] = (byte) b;
  }

  @Override
  public void flush() throws IOException {
    logger.debug("Flushing fragment");

    flushBuffer();
    outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    logger.debug("Closing fragment");

    flush();
    outputStream.close();
  }

  private void flushBuffer() throws IOException {
    logger.debug("Flushing buffer currentBufferPos:{} currentParityA:{} currentParityB:{}", new Object[] { currentBufferPos, currentParityA, currentParityB });

    outputStream.write(buffer, 0, currentBufferPos);
    currentBufferPos = 0;
    currentParityA = 0;
    currentParityB = 0;
  }

}
