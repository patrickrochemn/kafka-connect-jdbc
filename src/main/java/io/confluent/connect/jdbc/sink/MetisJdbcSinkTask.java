package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetisJdbcSinkTask extends JdbcSinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MetisJdbcSinkTask.class);

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }

        // Group records by the target table based on the 'table' field in the record value
        Map<String, List<SinkRecord>> recordsByTable = new HashMap<>();
        for (SinkRecord record : records) {
            try {
                Struct valueStruct = record.value() instanceof Struct ? (Struct) record.value() : null;
                String tableName;

                if (valueStruct != null && valueStruct.schema().field("table") != null) {
                    tableName = valueStruct.getString("table");
                } else {
                    throw new DataException("Record value does not contain 'table' field or it is null.");
                }

                // Add record to the list for the appropriate table
                recordsByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(record);
            } catch (Exception e) {
                logger.error("Error processing record: {}", record, e);
                throw new DataException("Error processing record in MetisJdbcSinkTask", e);
            }
        }

        // Process records for each table separately
        for (Map.Entry<String, List<SinkRecord>> entry : recordsByTable.entrySet()) {
            String tableName = entry.getKey();
            Collection<SinkRecord> tableRecords = entry.getValue();

            logger.debug("Routing {} records to table {}", tableRecords.size(), tableName);

            // Use custom writer to route records to the appropriate table
        }

        // After processing all records, reset retries
        remainingRetries = config.maxRetries;
    }

}
