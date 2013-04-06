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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;

public class TestFragmentOutputStream {

  private ByteArrayOutputStream parentOutputStream;
  private FragmentOutputStream outputStream;

  @Before
  public void setUp() throws Exception {
    parentOutputStream = new ByteArrayOutputStream(1024);
    outputStream = new FragmentOutputStream(parentOutputStream, 8);
  }

  @Test
  public void testWrite() throws Exception {
    outputStream.write("This is a test record\n".getBytes());
    outputStream.flush();
    outputStream.close();

    byte[] bytes = parentOutputStream.toByteArray();

    Assert.assertNotNull(bytes);
    Assert.assertEquals(22, bytes.length);
    Assert.assertEquals("This is a test record\n", new String(bytes));
  }

}
