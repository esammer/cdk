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
package com.cloudera.data.hcatalog;

import com.cloudera.data.DatasetDescriptor;
import com.cloudera.data.MetadataProvider;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hcatalog.api.HCatClient;
import org.apache.hcatalog.api.HCatTable;
import org.apache.hcatalog.data.schema.HCatFieldSchema;

import java.io.IOException;
import java.util.List;

public class HCatalogMetadataProvider implements MetadataProvider {

  @Override
  public DatasetDescriptor load(String name) throws IOException {
    HCatClient hCatClient = HCatClient.create(new Configuration());

    HCatTable table = hCatClient.getTable("default", name);
    List<HCatFieldSchema> cols = table.getCols();

    for (HCatFieldSchema col : cols) {
      HCatFieldSchema.Type type = col.getType();
    }
    new DatasetDescriptor.Builder().schema()
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void save(String name, DatasetDescriptor descriptor) throws IOException {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean delete(String name) throws IOException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  private static Schema.Field hcatToAvro(HCatFieldSchema fieldSchema) {
    switch (fieldSchema.getType()) {
      case INT:
      case TINYINT:
      case SMALLINT:
        return new Schema.Field(fieldSchema.getName(), Schema.create(Schema.Type.INT), null, null);
      case BIGINT:
        return new Schema.Field(fieldSchema.getName(), Schema.create(Schema.Type.LONG), null, null);
      case BOOLEAN:
        return new Schema.Field(fieldSchema.getName(), Schema.create(Schema.Type.BOOLEAN), null, null);
        break;
      case FLOAT:
        return new Schema.Field(fieldSchema.getName(), Schema.create(Schema.Type.FLOAT), null, null);
        break;
      case DOUBLE:
        return new Schema.Field(fieldSchema.getName(), Schema.create(Schema.Type.DOUBLE), null, null);
        break;
      case STRING:
        return new Schema.Field(fieldSchema.getName(), Schema.create(Schema.Type.STRING), null, null);
        break;
      case ARRAY:
        return new Schema.Field(fieldSchema.getName(), Schema.createArray(fieldSchema.getArrayElementSchema().), null, null);
        break;
      case MAP:
        break;
      case STRUCT:
        break;
      case BINARY:
        break;
    }
    return null;
  }

}
