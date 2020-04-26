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

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;


public interface SqlserverToBigqueryPipelineOptions extends DataflowPipelineOptions {
  @Description("SqlServer host name")
  @Default.String("localhost")
  @Validation.Required
  ValueProvider<String>  getSqlServerHostName();
  void setSqlServerHostName(ValueProvider<String> sqlServerHostName);

  @Description("SqlServer user name")
  @Default.String("myunsername")
  @Validation.Required
  ValueProvider<String>  getSqlServerUserName();
  void setSqlServerUserName(ValueProvider<String> sqlServerUserName);

  @Description("SqlServer password")
  @Default.String("mypwd")
  @Validation.Required
  ValueProvider<String>  getSqlServerPassword();
  void setSqlServerPassword(ValueProvider<String> sqlServerPassword);

  @Description("SqlServer full table name - databasename.schema.tablename")
  @Default.String("")
  @Validation.Required
  ValueProvider<String>  getSqlServerTableName();
  void setSqlServerTableName(ValueProvider<String> sqlServerTableName);

  @Description("Build number")
  ValueProvider<String> getBuildNumber();
  void setBuildNumber(ValueProvider<String> buildNumber);

  @Description("BigQuery destination dataset name")
  @Default.String("mydataset")
  @Validation.Required
  ValueProvider<String>  getBigQueryDatasetName();
  void setBigQueryDatasetName(ValueProvider<String> bigQueryDatasetName);

  @Description("BigQuery destination table name")
  @Default.String("mytable")
  @Validation.Required
  ValueProvider<String>  getBigQueryTableName();
  void setBigQueryTableName(ValueProvider<String> bigQueryTableName);

}