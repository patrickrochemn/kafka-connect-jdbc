package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import java.util.Collection;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetisJdbcSinkTask extends JdbcSinkTask {

    private static final Logger logger = LoggerFactory.getLogger(MetisJdbcSinkTask.class);

    @Override
    public void put(Collection<SinkRecord> records) {
        Collection<SinkRecord> modifiedRecords = new ArrayList<>();

        for (SinkRecord record : records) {
            try {
                // Assuming the record value is a Struct and the table name is stored in a field named "table"
                Struct valueStruct = (Struct) record.value();
                String tableName = valueStruct.getString("table");

                if (tableName != null && !tableName.isEmpty()) {
                    // Modify the record in a way that routes it to the correct table
                    // This example sets the record's topic to the table name
                    SinkRecord modifiedRecord = new SinkRecord(tableName, record.kafkaPartition(),
                            record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp());
                    modifiedRecords.add(modifiedRecord);
                } else {
                    // Handle records without a table field or with an empty table name
                    logger.warn("Record does not contain 'table' field or it is empty. Skipping record: {}", record);
                }
            } catch (Exception e) {
                // Handle potential exceptions, such as ClassCastException if the record value is not a Struct
                logger.error("Error processing record: {}", record, e);
                throw new DataException("Error processing record in MetisJdbcSinkTask", e);
            }
        }

        // Pass the modified records to the superclass's put method for actual writing to the database
        super.put(modifiedRecords);
    }

    // Implement other methods required by the JdbcSinkTask if necessary
    // You might need to override methods like start, stop, version, etc., depending on your specific requirements
}
