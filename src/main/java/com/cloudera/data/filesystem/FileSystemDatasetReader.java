package com.cloudera.data.filesystem;

import java.io.Closeable;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.data.DatasetReader;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

public class FileSystemDatasetReader<E> implements DatasetReader<E>, Closeable {

  private FileSystem fileSystem;
  private Path path;
  private Schema schema;

  private State state;
  private DataFileReader<E> reader;

  private static final Logger logger = LoggerFactory
      .getLogger(FileSystemDatasetReader.class);

  public FileSystemDatasetReader(FileSystem fileSystem, Path path, Schema schema) {
    this.fileSystem = fileSystem;
    this.path = path;
    this.schema = schema;

    this.state = State.NEW;
  }

  @Override
  public void open() throws IOException {
    Preconditions.checkState(state.equals(State.NEW),
        "A reader may not be opened more than once - current state:%s", state);

    reader = new DataFileReader<E>(new AvroFSInput(fileSystem.open(path),
        fileSystem.getFileStatus(path).getLen()), new ReflectDatumReader<E>());

    state = State.OPEN;
  }

  @Override
  public boolean hasNext() {
    return reader.hasNext();
  }

  @Override
  public E read() {
    Preconditions.checkState(state.equals(State.OPEN),
        "Attempt to read from a file in state:%s", state);

    return reader.next();
  }

  @Override
  public void close() throws IOException {
    if (!state.equals(State.OPEN)) {
      return;
    }

    logger.debug("Closing reader on path:{}", path);

    reader.close();
    state = State.CLOSED;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("path", path).add("state", state)
        .add("reader", reader).toString();
  }

  private static enum State {
    NEW, OPEN, CLOSED
  }

}