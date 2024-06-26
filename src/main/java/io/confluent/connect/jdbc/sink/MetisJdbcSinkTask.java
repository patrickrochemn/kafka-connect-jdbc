package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.crypto.Data;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.LogUtil;

public class MetisJdbcSinkTask extends JdbcSinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MetisJdbcSinkTask.class);

    @Override
    public void start(final Map<String, String> props) {
        logger.info("METIS: Starting Metis JDBC Sink task");
        config = new JdbcSinkConfig(props);
        initializePrimaryKeyCache(); // initialize the primary key cache for the db
        initWriter();
        remainingRetries = config.maxRetries;
        shouldTrimSensitiveLogs = config.trimSensitiveLogsEnabled;
        try {
         reporter = context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            // Will occur in Connect runtimes earlier than 2.6
            reporter = null;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
        return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        logger.debug(
            "METIS: Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
            + "database...",
            recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
        writer.write(records);
        } catch (TableAlterOrCreateException tace) {
        if (reporter != null) {
            unrollAndRetry(records);
        } else {
            logger.error(tace.toString());
            throw tace;
        }
        } catch (SQLException sqle) {
        SQLException trimmedException = shouldTrimSensitiveLogs
                ? LogUtil.trimSensitiveData(sqle) : sqle;
                logger.warn(
            "METIS: Write of {} records failed, remainingRetries={}",
            records.size(),
            remainingRetries,
            trimmedException
        );
        int totalExceptions = 0;
        for (Throwable e :sqle) {
            totalExceptions++;
        }
        SQLException sqlAllMessagesException = getAllMessagesException(sqle);
        if (remainingRetries > 0) {
            writer.closeQuietly();
            initWriter();
            remainingRetries--;
            context.timeout(config.retryBackoffMs);
            logger.debug(sqlAllMessagesException.toString());
            throw new RetriableException(sqlAllMessagesException);
        } else {
            if (reporter != null) {
            unrollAndRetry(records);
            } else {
                logger.error(
                "METIS: Failing task after exhausting retries; "
                    + "encountered {} exceptions on last write attempt. "
                    + "For complete details on each exception, please enable DEBUG logging.",
                totalExceptions);
            int exceptionCount = 1;
            for (Throwable e : trimmedException) {
                logger.debug("METIS: Exception {}:", exceptionCount++, e);
            }
            throw new ConnectException(sqlAllMessagesException);
            }
        }
        }
        remainingRetries = config.maxRetries;
    }

    private void unrollAndRetry(Collection<SinkRecord> records) {
        writer.closeQuietly();
        initWriter();
        for (SinkRecord record : records) {
        try {
            writer.write(Collections.singletonList(record));
        } catch (TableAlterOrCreateException tace) {
            logger.debug(tace.toString());
            reporter.report(record, tace);
            writer.closeQuietly();
        } catch (SQLException sqle) {
            SQLException sqlAllMessagesException = getAllMessagesException(sqle);
            logger.debug(sqlAllMessagesException.toString());
            reporter.report(record, sqlAllMessagesException);
            writer.closeQuietly();
        }
        }
    }

    private SQLException getAllMessagesException(SQLException sqle) {
        String sqleAllMessages = "METIS: Exception chain:" + System.lineSeparator();
        SQLException trimmedException = shouldTrimSensitiveLogs
                ? LogUtil.trimSensitiveData(sqle) : sqle;
        for (Throwable e : trimmedException) {
        sqleAllMessages += e + System.lineSeparator();
        }
        SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
        sqlAllMessagesException.setNextException(trimmedException);
        return sqlAllMessagesException;
    }

    @Override
    void initWriter() {
        logger.info("METIS: Initializing Metis JDBC writer");
        if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
        dialect = DatabaseDialects.create(config.dialectName, config);
        } else {
        dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        logger.info("METIS: Initializing Metis writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new MetisJdbcDbWriter(config, dialect, dbStructure, primaryKeyCache);
        logger.info("METIS: Metis JDBC writer initialized");
    }

    private Map<String, String> primaryKeyCache = new HashMap<>();

    // Getter for the primary key cache
    public Map<String, String> getPrimaryKeyCache() {
        return primaryKeyCache;
    }

    public void initializePrimaryKeyCache() {
        logger.info("METIS: Initializing primary key cache");
        // get the schema for the db and cache the primary keys
        try (Connection conn = DriverManager.getConnection(config.connectionUrl, config.connectionUser, config.connectionPassword)) {
            DatabaseMetaData metaData = conn.getMetaData();
            String catalog = null; // or specific catalog if needed
            String schemaPattern = "public"; // TODO: make this configurable

            try (ResultSet tables = metaData.getTables(catalog, schemaPattern, null, new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    try (ResultSet pkRs = metaData.getPrimaryKeys(catalog, schemaPattern, tableName)) {
                        while (pkRs.next()) {
                            String pkColumnName = pkRs.getString("COLUMN_NAME");
                            primaryKeyCache.put(tableName, pkColumnName);
                            // logger.info("Cached primary key for table {}: {}", tableName, pkColumnName);
                        }
                    }
                }
            }

            // print out the cache in one log statement
            logger.info("METIS: Primary key cache: {}", primaryKeyCache);

        } catch (SQLException e) {
            logger.error("METIS: Error initializing primary key cache", e);
            throw new ConnectException("METIS: Error initializing primary key cache", e);
        }
    }

}
