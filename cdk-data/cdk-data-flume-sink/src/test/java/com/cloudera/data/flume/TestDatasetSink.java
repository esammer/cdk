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
package com.cloudera.data.flume;

import com.cloudera.cdk.data.flume.DatasetSink;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

public class TestDatasetSink {

  @Test
  public void testProcess() throws EventDeliveryException {
    DatasetSink datasetSink = new DatasetSink();

    Context context = new Context();
    context.put("capacity", "100");
    context.put("uri", "datarepo:file:///tmp/target");
    context.put("dataset.name", "test");

    MemoryChannel channel = new MemoryChannel();
    channel.configure(context);

    datasetSink.setChannel(channel);
    datasetSink.configure(context);
    datasetSink.start();

    Sink.Status status = datasetSink.process();

    Assert.assertEquals(Sink.Status.BACKOFF, status);

    Transaction transaction = channel.getTransaction();
    transaction.begin();
    channel.put(EventBuilder.withBody(new byte[] { }));
    transaction.commit();
    transaction.close();

    datasetSink.process();

    datasetSink.stop();
  }

}
