package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.jdbc.sink.JdbcSinkTask.log;

import java.sql.SQLException;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.util.LogUtil;

public class MetisJdbcSinkTask extends JdbcSinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MetisJdbcSinkTask.class);

    @Override
    public void start(final Map<String, String> props) {
        logger.info("Starting Metis JDBC Sink task");
        config = new JdbcSinkConfig(props);
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
            "Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the "
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
            "Write of {} records failed, remainingRetries={}",
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
                "Failing task after exhausting retries; "
                    + "encountered {} exceptions on last write attempt. "
                    + "For complete details on each exception, please enable DEBUG logging.",
                totalExceptions);
            int exceptionCount = 1;
            for (Throwable e : trimmedException) {
                logger.debug("Exception {}:", exceptionCount++, e);
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
        String sqleAllMessages = "Exception chain:" + System.lineSeparator();
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
        logger.info("Initializing Metis JDBC writer");
        if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
        dialect = DatabaseDialects.create(config.dialectName, config);
        } else {
        dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        }
        final DbStructure dbStructure = new DbStructure(dialect);
        logger.info("Initializing Metis writer using SQL dialect: {}", dialect.getClass().getSimpleName());
        writer = new MetisJdbcDbWriter(config, dialect, dbStructure);
        logger.info("Metis JDBC writer initialized");
    }

}
