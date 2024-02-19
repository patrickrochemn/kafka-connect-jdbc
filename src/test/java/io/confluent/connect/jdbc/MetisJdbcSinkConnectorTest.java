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

package io.confluent.connect.jdbc;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import static io.confluent.connect.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import org.apache.kafka.connect.errors.ConnectException;

import org.junit.Test;

import io.confluent.connect.jdbc.sink.MetisJdbcSinkTask;

public class MetisJdbcSinkConnectorTest {

  @Test
  public void testValidationWhenDeleteEnabled() {

    MetisJdbcSinkConnector connector = new MetisJdbcSinkConnector();

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.MetisJdbcSinkConnector");
    connConfig.put("delete.enabled", "true");

    connConfig.put("pk.mode", "record_key");
    assertEquals("'pk.mode must be 'RECORD_KEY/record_key' when 'delete.enabled' == true",
        EMPTY_LIST, configErrors(connector.validate(connConfig), PK_MODE));

    connConfig.put("pk.mode", "RECORD_KEY");
    assertEquals("pk.mode must be 'RECORD_KEY/record_key' when 'delete.enabled' == true",
        EMPTY_LIST, configErrors(connector.validate(connConfig), PK_MODE));

    connConfig.put("pk.mode", "none");

    final String conflictMsg = "Deletes are only supported for pk.mode record_key";

    assertEquals("'record_key' is the only valid mode when 'delete.enabled' == true",
        singletonList(conflictMsg),
        configErrors(connector.validate(connConfig), PK_MODE));
  }

  @Test
  public void testValidationWhenDeleteNotEnabled() {

    MetisJdbcSinkConnector connector = new MetisJdbcSinkConnector();

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.MetisJdbcSinkConnector");
    connConfig.put("delete.enabled", "false");

    connConfig.put("pk.mode", "none");
    assertEquals("any defined mode is valid when 'delete.enabled' == false",
        EMPTY_LIST, configErrors(connector.validate(connConfig), PK_MODE));
  }

  @Test
  public void testValidationWhenPKModeInvalid() {

    MetisJdbcSinkConnector connector = new MetisJdbcSinkConnector();

    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.MetisJdbcSinkConnector");
    connConfig.put("delete.enabled", "false");
    connConfig.put("pk.mode", "gibberish");

    assertEquals("no double reporting for unknown pk.mode",
        1, configErrors(connector.validate(connConfig), PK_MODE).size());
  }


  private List<String> configErrors(Config config, String propertyName) {
    return config.configValues()
        .stream()
        .flatMap(cfg -> propertyName.equals(cfg.name()) ?
            cfg.errorMessages().stream() : Stream.empty())
        .collect(Collectors.toList());
  }

  @Test
  public void testDynamicTableRouting() {
    MetisJdbcSinkConnector connector = new MetisJdbcSinkConnector();
    Map<String, String> connConfig = new HashMap<>();
    connConfig.put("connector.class", "io.confluent.connect.jdbc.MetisJdbcSinkConnector");
    connConfig.put("auto.create", "true");
    connConfig.put("auto.evolve", "true");

    // Mock a SinkRecord with a 'table' field
    SinkRecord mockRecord = createMockRecordWithTableField("my_dynamic_table");
    Collection<SinkRecord> records = Collections.singletonList(mockRecord);

    // Initialize connector and task

    // Invoke the "put" method with the mocked record

    // Verify that the record was routed to "my_dynamic_table"
  }

  private SinkRecord createMockRecordWithTableField(String tableName) {
    return new SinkRecord("my_topic", 0, null, null, null, null, 0);
  }

  @Test(expected = DataException.class)
  public void testMissingTableField() {
      MetisJdbcSinkConnector connector = new MetisJdbcSinkConnector();
      Map<String, String> connConfig = new HashMap<>();
      connConfig.put("connector.class", "io.confluent.connect.jdbc.MetisJdbcSinkConnector");

      // Mock a SinkRecord without a 'table' field
      SinkRecord mockRecord = createMockRecordWithoutTableField();
      Collection<SinkRecord> records = Collections.singletonList(mockRecord);

      connector.start(connConfig);
      List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
      MetisJdbcSinkTask task = new MetisJdbcSinkTask();
      task.start(taskConfigs.get(0));

      // Expect a DataException due to the missing 'table' field
      task.put(records);
  }

  private SinkRecord createMockRecordWithoutTableField() {
    return new SinkRecord("my_topic", 0, null, null, null, null, 0);
  }

  @Test(expected = ConnectException.class)
  public void testInvalidTableName() {
      MetisJdbcSinkConnector connector = new MetisJdbcSinkConnector();
      Map<String, String> connConfig = new HashMap<>();
      connConfig.put("connector.class", "io.confluent.connect.jdbc.MetisJdbcSinkConnector");

      // Mock a SinkRecord with an invalid 'table' field value
      SinkRecord mockRecord = createMockRecordWithTableField("invalid_table_name!");
      Collection<SinkRecord> records = Collections.singletonList(mockRecord);

      connector.start(connConfig);
      List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
      MetisJdbcSinkTask task = new MetisJdbcSinkTask();
      task.start(taskConfigs.get(0));

      // Expect a ConnectException due to the invalid table name
      task.put(records);
  }

}


