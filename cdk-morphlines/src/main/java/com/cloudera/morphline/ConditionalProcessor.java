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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.List;
import java.util.Map;

public class ConditionalProcessor implements Processor {

  private Predicate<Object> predicate;
  private Processor processor;

  public ConditionalProcessor(Predicate<Object> predicate, Processor processor) {
    Preconditions.checkArgument(predicate != null, "Predicate may not be null");
    Preconditions.checkArgument(processor != null, "Processor may not be null");

    this.predicate = predicate;
    this.processor = processor;
  }

  @Override
  public void process(Map<String, Object> record, List<Map<String, Object>> results) {
    if (predicate.apply(record)) {
      processor.process(record, results);
    }
  }

}
