/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.examples;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableReference;

import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Class {@link org.apache.examples.SqlserverToBigqueryPipeline} is a driver class to run pipeline to process
 * that copies data from Sql Server to BigQuery using JDBC drivers.
 * This pipeline assumes that BigQuery table has a same schema as Sql Server table. Pipeline assumes that Bigquery
 * table already exists.
 * */
public class SqlserverToBigqueryPipeline {
     /*   mvn compile exec:java -Dexec.mainClass=org.apache.examples.SqlserverSinkPipeline \
           -Dexec.args="--project=google.com:testdhaval \
                        --stagingLocation=gs://dd_dataflow_stage/stage \
                        --gcpTempLocation=gs://dd_dataflow_temp/temp \
                        --runner=DataflowRunner \
                        --autoscalingAlgorithm=THROUGHPUT_BASED \
                        --maxNumWorkers=10 \
                        --sqlServerHostName=10.14.128.3 \
                        --sqlServerUserName=sqlserver \
                        --sqlServerPassword=scmin.12345 \
                        --sqlServerTableName="sample_db.dbo.persons" \
                        --bigQueryDatasetName=db \
                        --bigQueryTableName=persons \
                        --jobName=sql-server-pipeline-dev-v1"
        To create a template:
        mvn compile exec:java \
            -Dexec.mainClass=org.apache.streaming.pipeline.SqlserverSinkPipeline \
            -Dexec.args="--runner=DataflowRunner \
                        --templateLocation=gs://dd_export/templates/SqlServerToBqPipeline1.0 \
                        --project=google.com:testdhaval \
                        --stagingLocation=gs://dd_dataflow_stage/stage \
                        --gcpTempLocation=gs://dd_dataflow_temp/temp \
                        --autoscalingAlgorithm=THROUGHPUT_BASED \
                        --maxNumWorkers=10 \
                        --sqlServerHostName=10.14.128.3 \
                        --sqlServerUserName=sqlserver \
                        --sqlServerPassword=scmin.12345 \
                        --sqlServerTableName="sample_db.dbo.persons" \
                        --bigQueryDatasetName=db \
                        --bigQueryTableName=persons \
                        --jobName=sql-server-pipeline-dev-v1"
    */
  private static final BigQueryIO.Write.CreateDisposition
    BQ_CREATE_DISPOSITION = BigQueryIO.Write.CreateDisposition.CREATE_NEVER;
  private static final BigQueryIO.Write.WriteDisposition
    BQ_WRITE_DISPOSITION = BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;

  private static final Logger LOG = LoggerFactory.getLogger(SqlserverToBigqueryPipeline.class);

  // main class
  public static void main(String[] args){
    // Register Options class for our pipeline with the factory
    PipelineOptionsFactory.register(SqlserverToBigqueryPipelineOptions.class);
    SqlserverToBigqueryPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
      .withValidation()
      .as(SqlserverToBigqueryPipelineOptions.class);

    // Get options for SQL server and BQ
    final String GCP_PROJECT_NAME = options.getProject();
    final ValueProvider<String> GCP_BQ_DATASET_NAME = options.getBigQueryDatasetName();
    final ValueProvider<String> GCP_BQ_TABLE_NAME = options.getBigQueryTableName();
    final ValueProvider<String> SQL_HOST_NAME = options.getSqlServerHostName();
    final ValueProvider<String> SQL_USER_NAME = options.getSqlServerUserName();
    final ValueProvider<String> SQL_PASSWORD = options.getSqlServerPassword();
    final ValueProvider<String> SQL_TABLE_NAME = options.getSqlServerTableName();

    // Build JDBC url
    String jdbcUrl = "jdbc:sqlserver://"+ SQL_HOST_NAME.get() +";" +
            "user="+SQL_USER_NAME.get()+";" +
            "password="+SQL_PASSWORD.get();

    // Build a sample sql query
    String sqlQuery = "SELECT * from " + SQL_TABLE_NAME.get();

    options.setTempLocation(options.getGcpTempLocation());
    // Create a pipeline using options created above
    Pipeline p = Pipeline.create(options);

    // Read from Sql server
    p.apply("SQLServer Read - JDBC",JdbcIO.<TableRow>read()
            .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                    "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    jdbcUrl)
            )
            .withQuery(sqlQuery)
            .withCoder(TableRowJsonCoder.of())
            .withRowMapper(new JdbcIO.RowMapper<TableRow>() {
              public TableRow mapRow(ResultSet resultSet) throws Exception {
                return getBqRowForTable(resultSet);
              }
            })
    ).apply("BQ write",BigQueryIO.writeTableRows()
            .to(new TableReference()
                    .setProjectId(GCP_PROJECT_NAME)
                    .setDatasetId(GCP_BQ_DATASET_NAME.get())
                    .setTableId(GCP_BQ_TABLE_NAME.get()))
            .withWriteDisposition(BQ_WRITE_DISPOSITION)
            .withCreateDisposition(BQ_CREATE_DISPOSITION));

    p.run();
  }

  // Build a TableRow for SqlServer table
  private static TableRow getBqRowForTable(ResultSet resultSet) throws SQLException {
    ResultSetMetaData md = resultSet.getMetaData();

    // SQL starts counting with 1 instead of 0
    int cnt = 1;
    TableRow tr = new TableRow();

    // Go through all columns and build a row for BigQuery
    while(cnt <= md.getColumnCount()){
      tr.set(md.getColumnName(cnt), resultSet.getString(cnt));
      cnt +=1;
    }
    return tr;
  }
}