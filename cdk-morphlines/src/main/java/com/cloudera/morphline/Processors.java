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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.Map;

public class Processors {

  public static Processor toFilterProcessor(final Function<Object, Boolean> function) {
    Preconditions.checkArgument(function != null, "Function may not be null");

    return new FilterProcessor(new Predicate<Map<String, Object>>() {
      @Override
      public boolean apply(Map<String, Object> input) {
        return function.apply(input);
      }
    });
  }

}
