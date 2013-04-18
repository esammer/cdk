package com.cloudera.morphline;

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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Morphline implements Processor {

  private static final Logger logger = LoggerFactory.getLogger(Morphline.class);

  private List<Processor> processors;

  public Morphline(List<Processor> processors) {
    Preconditions.checkArgument(processors != null, "Processors may not be null");

    this.processors = processors;
  }

  @Override
  public void process(Map<String, Object> record, List<Map<String, Object>> results) {
    Preconditions.checkArgument(results != null, "Results may not be null");

    logger.debug("Process record:{} results:{}", record, results);

    for (Processor processor : processors) {
      logger.debug("execute processor:{}", processor);

      processor.process(record, results);
    }
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("processors", processors)
      .toString();
  }

  public static class Builder implements Supplier<Morphline> {

    private List<Processor> processors;

    public Builder() {
      processors = Lists.newArrayList();
    }

    public Builder processors(List<Processor> processors) {
      this.processors = processors;
      return this;
    }

    @Override
    public Morphline get() {
      Morphline morphline = new Morphline(processors);

      return morphline;
    }
  }

}
