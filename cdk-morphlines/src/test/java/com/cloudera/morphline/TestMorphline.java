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
package com.cloudera.morphline;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TestMorphline {

  private static final Logger logger = LoggerFactory.getLogger(TestMorphline.class);

  @Test
  public void testProcessEmpty() throws Exception {
    Morphline morphline = new Morphline.Builder().get();

    logger.debug("morphline:{}", morphline);

    Map<String, Object> record = Maps.newHashMap();
    List<Map<String, Object>> results = Lists.newArrayList();

    record.put("message", "Test");

    morphline.process(record, results);

    Assert.assertNotNull(results);
    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testProcessFilter() {
    Morphline morphline = new Morphline.Builder().processors(
      Lists.<Processor>newArrayList(
        new FilterProcessor(Predicates.<Map<String, Object>>alwaysTrue()),
        new FilterProcessor(Predicates.<Map<String, Object>>notNull())
      )
    ).get();

    Map<String, Object> record = Maps.newHashMap();
    List<Map<String, Object>> results = Lists.newArrayList();

    record.put("message", "Test");

    morphline.process(record, results);

    Assert.assertNotNull(results);
    Assert.assertEquals(1, results.size());
    Assert.assertEquals("Test", results.get(0).get("message"));
  }

}
