package com.cloudera.cdk.data.flume;

import com.cloudera.cdk.data.Dataset;
import com.cloudera.cdk.data.DatasetRepository;
import com.cloudera.cdk.data.DatasetWriter;
import com.cloudera.cdk.data.filesystem.FileSystemDatasetRepository;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class DatasetSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory.getLogger(DatasetSink.class);

  private URI uri;
  private String datasetName;

  private DatasetRepository datasetRepo;
  private Dataset dataset;
  private DatasetWriter<GenericRecord> writer;

  private Schema schema; // Cached dataset schema. Optimization.
  private BinaryDecoder datumDecoder; // Cached event payload decoder. Optimization.
  private DatumReader<GenericRecord> datumReader; // Cached event payload reader. Optimization.

  @Override
  public void configure(Context context) {
    String uriString = context.getString("uri");

    Preconditions.checkArgument(uriString != null, "Missing required uri parameter");

    try {
      uri = new URI(uriString);

      logger.debug("uri property:{} authority:{}", uri, uri.getAuthority());

      Preconditions.checkArgument(uri.getScheme().equals("datarepo"),
        "Invalid dataset URI:" + uriString + " - URI scheme must be datarepo");

      String schemeSpecificPart = uri.getSchemeSpecificPart();

      URI specific = new URI(schemeSpecificPart);

      if (specific.getScheme().equals("hdfs")) {
        datasetRepo = new FileSystemDatasetRepository(specific);
      } else if (specific.getScheme().equals("file")) {
        datasetRepo = new FileSystemDatasetRepository(specific);
      } else if (specific.getScheme().equals("hive")) {
        throw new UnsupportedOperationException("HCatalog/Hive dataset repositories are not yet support (uri:" + uriString + ")");
      } else {
        throw new IllegalArgumentException("Invalid dataset URI:" + uriString + " - Unknown sub-scheme:" + specific.getScheme());
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Invalid dataset URI:" + uriString + " - Malformed URI syntax:" + e.getMessage());
    }

    datasetName = context.getString("dataset.name");

    Preconditions.checkNotNull(datasetName, "Missing required dataset.name parameter");
  }

  @Override
  public synchronized void start() {
    super.start();

    dataset = datasetRepo.load(datasetName);
    writer = dataset.getWriter();

    schema = dataset.getDescriptor().getSchema();
    datumReader = new GenericDatumReader<GenericRecord>(schema);
  }

  @Override
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();

    transaction.begin();

    try {
      Event event = channel.take();

      if (event != null) {
        datumDecoder = DecoderFactory.get().binaryDecoder(event.getBody(), datumDecoder);
        GenericRecord entity = datumReader.read(null, datumDecoder);
        writer.write(entity);

        transaction.commit();

        return Status.READY;
      } else {
        transaction.rollback();
      }
    } catch (RuntimeException e) {
      transaction.rollback();
      throw e;
    } catch (IOException e) {
      e.printStackTrace();  // TODO: Unhandled catch block!
    } finally {
      transaction.close();
    }

    return Status.BACKOFF;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("datasetName", datasetName)
      .add("dataset", dataset)
      .toString();
  }
}
