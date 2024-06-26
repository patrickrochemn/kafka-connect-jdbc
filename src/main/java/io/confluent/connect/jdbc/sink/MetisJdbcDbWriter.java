/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Struct;

public class MetisJdbcDbWriter extends JdbcDbWriter{
  private static final Logger log = LoggerFactory.getLogger(MetisJdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private Map<String, String> primaryKeyCache = new HashMap<>();
  final CachedConnectionProvider cachedConnectionProvider;

  public MetisJdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure, Map<String, String> primaryKeyCache) {
    super(config, dbDialect, dbStructure);
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.primaryKeyCache = primaryKeyCache;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  private SinkRecord adjustRecord(SinkRecord originalRecord, String tableName) {
    if (originalRecord.value() instanceof Struct) {
      Struct originalValue = (Struct) originalRecord.value();

      // Schema for the modified record value
      SchemaBuilder builder = SchemaBuilder.struct();
      for (Field field: originalValue.schema().fields()) {
        if (!field.name().equals("table")) { // Excluding 'table' field
          builder.field(field.name(), field.schema());
        }
      }
      Schema newValueSchema = builder.build();
      Struct newValue = new Struct(newValueSchema);
      for (Field field : newValueSchema.fields()) {
        newValue.put(field.name(), originalValue.get(field.name()));
      }

      // Handling the primary key
      String primaryKey = primaryKeyCache.getOrDefault(tableName, "Id"); // Defaulting to "Id"
      Object newKey = originalValue.get(primaryKey);
      if (newKey == null) {
        log.warn("Primary key '{}' not found in record for table '{}'. Using original key", primaryKey, tableName);
        newKey = originalRecord.key(); // Fallback to original key if PK not found
      }

      // Logging for debugging
      log.debug("Adjusting record for table '{}' with new key '{}' based on PK '{}'.", tableName, newKey, primaryKey);

      return new SinkRecord(
        originalRecord.topic(),
        originalRecord.kafkaPartition(),
        Schema.STRING_SCHEMA, // Assuming String schema for PK
        newKey,
        newValueSchema,
        newValue,
        originalRecord.kafkaOffset(),
        originalRecord.timestamp(),
        originalRecord.timestampType()
      );
    } else {
      log.error("Record value is not a Struct or missing 'table' field, returning original record.");
      return originalRecord; // Return original if not a Struct or no 'table' field
    }
  }

  @SuppressWarnings("finally")
  void write(final Collection<SinkRecord> records)
    throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        String tableName = extractTableName(record); // Extract the table name from record
        SinkRecord adjustedRecord = adjustRecord(record, tableName); // Adjust record to exclude 'table' field
        BufferedRecords buffer = bufferByTable.get(tableName); // buffer by tableName instead of tableId
        if (buffer == null) {
          TableId tableId = dbDialect.parseTableIdentifier(tableName);
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableName, buffer);
        }
        buffer.add(adjustedRecord);
      }
      for (BufferedRecords buffer : bufferByTable.values()) {
        buffer.flush();
        buffer.close();
      }
      // TODO: verify if this is where we will put in custom ATC handling
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      log.error("Write of records failed. In first level of catch block. Rolling back.", e);
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        log.error("Rollback of records failed. In second level of catch block. Ignoring.", sqle);
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  private String extractTableName(SinkRecord record) {
    // Assuming the record value is a Struct and the table name is stored in a field named "table"
    if (record.value() instanceof Struct) {
      Struct valueStruct = (Struct) record.value();
      // Log the extracted table name
      // log.info("Extracted table name: " + valueStruct.getString("table"));
      return valueStruct.getString("table");
    } else {
      // if it isn't a Struct, throw an exception and say what the class is
      log.error("Record value must be a Struct. Was " + record.value().getClass());
      throw new ConnectException("Record value must be a Struct. Was " + record.value().getClass());
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
