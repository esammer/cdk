// (c) Copyright 2011-2013 Cloudera, Inc.
package com.cloudera.cdk.data.hbase;

import org.apache.hadoop.hbase.client.HTablePool;

import com.cloudera.cdk.data.hbase.transactions.TransactionManager;

/**
 * A DAO implementation that uses a constructor provided EntityMapper to do
 * basic conversion between entities and HBase Gets and Puts.
 *
 * @param <K>
 *          The key type
 * @param <E>
 *          The entity type.
 */
public class BaseDao<K, E> implements Dao<K, E> {

  private final EntityMapper<K, E> entityMapper;
  private final HBaseClientTemplate clientTemplate;
  private final boolean transactional;

  /**
   * Constructor that will internally create an HBaseClientTemplate from the
   * tablePool and the tableName.
   *
   * @param transactionManager
   *          The TransactionManager that will manage transactional entities.
   * @param tablePool
   *          A pool of HBase Tables.
   * @param tableName
   *          The name of the table this dao persists to and fetches from.
   * @param entityMapper
   *          Maps between entities and the HBase operations.
   */
  public BaseDao(TransactionManager transactionManager, HTablePool tablePool,
      String tableName, EntityMapper<K, E> entityMapper) {
    this.entityMapper = entityMapper;
    this.clientTemplate = new HBaseClientTemplate(transactionManager,
        tablePool, tableName);
    this.transactional = entityMapper.getEntitySchema().isTransactional();
  }

  /**
   * Constructor that copies an existing dao.
   *
   * @param dao
   *          Dao to copy.
   */
  public BaseDao(BaseDao<K, E> dao) {
    this.clientTemplate = new HBaseClientTemplate(dao.clientTemplate);
    this.entityMapper = dao.entityMapper;
    this.transactional = dao.transactional;
  }

  @Override
  public E get(K key) {
    return clientTemplate.get(key, entityMapper, transactional);
  }

  @Override
  public boolean put(K key, E entity) {
    return clientTemplate.put(key, entity, entityMapper, transactional);
  }

  @Override
  public long increment(K key, String fieldName, long amount) {
    if (transactional) {
      // Atomic increment can't be done safely on transactional DAOs. Instead of
      // just allowing them to increment (with the potential for double counting
      // if a transaction is backed out and retried), we throw an exception.
      throw new HBaseCommonException(
          "Increment on transactional DAOs not supported.");
    }
    return clientTemplate.increment(key, fieldName, amount, entityMapper);
  }

  public void delete(K key) {
    clientTemplate.delete(key, entityMapper.getRequiredColumns(), null,
        entityMapper.getKeySerDe(), transactional);
  }

  @Override
  public boolean delete(K key, E entity) {
    VersionCheckAction checkAction = entityMapper.mapFromEntity(key, entity)
        .getVersionCheckAction();
    return clientTemplate.delete(key, entityMapper.getRequiredColumns(),
        checkAction, entityMapper.getKeySerDe(), transactional);
  }

  @Override
  public EntityScanner<K, E> getScanner() {
    return getScanner((K)null, null);
  }

  @Override
  public EntityScanner<K, E> getScanner(K startKey, K stopKey) {
    return clientTemplate.getScannerBuilder(entityMapper, transactional)
      .setStartKey(startKey)
      .setStopKey(stopKey)
      .build();
  }

  @Override
  public EntityScanner<K, E> getScanner(K startKey, K stopKey,
      ScanModifier scanModifier) {
    return clientTemplate.getScannerBuilder(entityMapper, transactional)
        .setStartKey(startKey)
        .setStopKey(stopKey)
        .addScanModifier(scanModifier)
        .build();
  }

  @Override
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey,
      PartialKey<K> stopKey) {
    return clientTemplate.getScannerBuilder(entityMapper, transactional)
        .setPartialStartKey(startKey)
        .setPartialStopKey(stopKey)
        .build();
  }

  @Override
  public EntityScanner<K, E> getScanner(PartialKey<K> startKey,
      PartialKey<K> stopKey, ScanModifier scanModifier) {
    return clientTemplate.getScannerBuilder(entityMapper, transactional)
        .setPartialStartKey(startKey)
        .setPartialStopKey(stopKey)
        .addScanModifier(scanModifier)
        .build();
  }  
  
  @Override
  public EntityScannerBuilder<K, E> getScannerBuilder() {
    return clientTemplate.getScannerBuilder(entityMapper, transactional);
  }

  @Override
  public EntityBatch<K, E> newBatch(long writeBufferSize) {
    return clientTemplate.createBatch(entityMapper, writeBufferSize);
  }

  @Override
  public EntityBatch<K, E> newBatch() {
    return clientTemplate.createBatch(entityMapper);
  }

  /**
   * Get the HBaseClientTemplate instance this DAO is using to interact with
   * HBase.
   *
   * @return The HBaseClientTemplate instance.
   */
  public HBaseClientTemplate getHBaseClientTemplate() {
    return clientTemplate;
  }

  @Override
  public KeySchema<?> getKeySchema() {
    return entityMapper.getKeySchema();
  }

  @Override
  public EntitySchema<?> getEntitySchema() {
    return entityMapper.getEntitySchema();
  }
  
  @Override
  public KeySerDe<K> getKeySerDe() {
    return entityMapper.getKeySerDe();
  }
  
  @Override
  public EntitySerDe<E> getEntitySerDe() {
    return entityMapper.getEntitySerDe();
  }

  @Override
  public EntityMapper<K, E> getEntityMapper() {
    return this.entityMapper;
  }

}