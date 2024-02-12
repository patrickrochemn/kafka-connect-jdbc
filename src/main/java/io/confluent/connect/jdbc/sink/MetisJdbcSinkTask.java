import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import io.confluent.connect.jdbc.sink.JdbcSinkTask;
import java.util.Collection;
import java.util.ArrayList;

public class MetisJdbcSinkTask extends JdbcSinkTask {

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
                    // For example, log a warning and skip the record or add it to a default table
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

    // Override other necessary methods and add custom logic as needed
}
