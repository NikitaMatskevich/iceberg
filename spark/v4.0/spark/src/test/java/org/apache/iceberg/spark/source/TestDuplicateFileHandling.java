/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDuplicateFileHandling extends CatalogTestBase {

  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()));

  private static final Map<String, String> CATALOG_PROPS =
      ImmutableMap.of(
          "type", "hive",
          "default-namespace", "default",
          "cache-enabled", "false");

  @Parameter(index = 3)
  private FileFormat format;

  @Parameter(index = 4)
  private int formatVersion;

  @Parameters(
      name =
          "catalogName = {1}, implementation = {2}, config = {3}, fileFormat = {4}, formatVersion = {5}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET,
        2
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET,
        3
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        CATALOG_PROPS,
        FileFormat.PARQUET,
        4
      },
    };
  }

  @TestTemplate
  public void testDuplicateFileWithPositionalDeletes() throws IOException {
    String tableName = "duplicate_file_test";
    Table table = createTable(tableName, SCHEMA, PartitionSpec.unpartitioned());

    // Step 1: Create and write data file A
    DataFile dataFileA = createTestDataFile(table, "fileA");
    table.newAppend().appendFile(dataFileA).commit();

    // Step 2: Create another data file B and commit both A and B (creating duplicate A)
    DataFile dataFileB = createTestDataFile(table, "fileB");
    table.newAppend().appendFile(dataFileA).appendFile(dataFileB).commit();

    // Step 3: Verify duplicate records from file A
    Dataset<Row> df = spark.read().format("iceberg").load(catalogName + ".default." + tableName);

    // Verify that we have duplicated records from file A
    // File A appears twice (original + duplicate) + File B once = 9 total records
    assertThat(df.count()).isEqualTo(9);
    assertThat(df.filter("data = 'fileA_data1'").count()).isEqualTo(2);
    assertThat(df.filter("data = 'fileA_data2'").count()).isEqualTo(2);
    assertThat(df.filter("data = 'fileA_data3'").count()).isEqualTo(2);

    // Step 4: Apply positional delete to file A (delete record at pos 0)
    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    deletes.add(Pair.of(dataFileA.location(), 0L)); // Delete first record of file A

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes,
            formatVersion);

    table.newRowDelta().addDeletes(posDeletes.first()).commit();

    // Step 5: Verify positional delete effects
    Dataset<Row> postDeleteDf = spark.read().format("iceberg").load(catalogName + ".default." + tableName);

    // After positional delete of position 0 in file A:
    // - fileA_data1 was at position 0, so both instances should be deleted (2 -> 0)
    // - fileA_data2 and fileA_data3 should still appear twice (2 each)
    // Total: 0 + 2 + 2 + 1 + 1 + 1 = 7 records
    assertThat(postDeleteDf.count()).isEqualTo(7);
    assertThat(postDeleteDf.filter("data = 'fileA_data1'").count()).isEqualTo(0);
    assertThat(postDeleteDf.filter("data = 'fileA_data2'").count()).isEqualTo(2);
    assertThat(postDeleteDf.filter("data = 'fileA_data3'").count()).isEqualTo(2);

    table.newRewrite().deleteFile(dataFileA).addFile(dataFileA).commit();

    // Step 6: Post-rewrite verification
    Dataset<Row> postRewriteDf = spark.read().format("iceberg").load(catalogName + ".default." + tableName);
    assertThat(postRewriteDf.count()).isEqualTo(5);
    assertThat(postRewriteDf.filter("data = 'fileA_data1'").count()).isEqualTo(0);
    assertThat(postRewriteDf.filter("data = 'fileA_data2'").count()).isEqualTo(1);
    assertThat(postRewriteDf.filter("data = 'fileA_data3'").count()).isEqualTo(1);

    dropTable(tableName);
  }

  private DataFile createTestDataFile(Table table, String filePrefix) throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    List<Record> records = Lists.newArrayList(
        record.copy("id", 1, "data", filePrefix + "_data1"),
        record.copy("id", 2, "data", filePrefix + "_data2"),
        record.copy("id", 3, "data", filePrefix + "_data3")
    );

    return FileHelpers.writeDataFile(
        table,
        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
        TestHelpers.Row.of(),
        records);
  }


  protected Table createTable(String name, Schema schema, PartitionSpec spec) {
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.FORMAT_VERSION,
            String.valueOf(formatVersion),
            TableProperties.DEFAULT_FILE_FORMAT,
            format.toString());
    return validationCatalog.createTable(
        TableIdentifier.of("default", name), schema, spec, properties);
  }

  protected void dropTable(String name) {
    validationCatalog.dropTable(TableIdentifier.of("default", name), false);
  }
}