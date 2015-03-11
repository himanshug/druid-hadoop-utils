/**
* Copyright 2015 Yahoo! Inc. Licensed under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance with the License.
* You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
* See accompanying LICENSE file.
*/
package com.yahoo.druid.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;


/**
 * Hadoop InputFormat to read data from Druid stored on hdfs.
 * <br/>
 * You have to provide following in the job configuration.
 * <br/>
 * <ul>
 * <li>druid.overlord.hostport - overlord host:port</li>
 * <li>druid.storage.storageDirectory - hdfs storage directory for druid segments</li>
 * <li>druid.datasource - druid datasource name</li>
 * <li>druid.datasource.interval - time interval for the segments to be read</li>
 * </ul>
 * <br/>
 * And, either one of following.
 * <ul>
 * <li>druid.datasource.schemafile - a json file containing name of metrics and dimensions</li>
 * <li>druid.datasource.schema - json string containing name of metrics and dimensions</li>
 * </ul>
 * For json schema details, see {@link SegmentLoadSpec} 
 */
public class DruidInputFormat extends InputFormat<DateTime, Map<String,Object>>
{

  private static final Logger logger = LoggerFactory.getLogger(DruidInputFormat.class);

  public static final String CONF_DRUID_OVERLORD_HOSTPORT = "druid.overlord.hostport";
  public static final String CONF_DRUID_STORAGE_STORAGE_DIR = "druid.storage.storageDirectory";
  public static final String CONF_DRUID_DATASOURCE = "druid.datasource";
  public static final String CONF_DRUID_INTERVAL = "druid.datasource.interval";
  
  //out of following 2, only one should be specified
  public static final String CONF_DRUID_SCHEMA = "druid.datasource.schema";
  public static final String CONF_DRUID_SCHEMA_FILE = "druid.datasource.schemafile";
  
  private DruidHelper druid = new DruidHelper();
  
  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();
    final String overlordUrl = conf.get(CONF_DRUID_OVERLORD_HOSTPORT);
    final String storageDir = conf.get(CONF_DRUID_STORAGE_STORAGE_DIR);
    String dataSource = conf.get(CONF_DRUID_DATASOURCE);
    String intervalStr = conf.get(CONF_DRUID_INTERVAL);
    
    logger.info("druid overlord url = " + overlordUrl);
    logger.info("druid storage dir = " + storageDir);
    logger.info("druid datasource = " + dataSource);
    logger.info("druid datasource interval = " + intervalStr);
    
    //TODO: currently we are creating 1 split per segment which is not really
    //necessary, we can use some configuration to combine multiple segments into
    //one input split
    List<InputSplit> splits =  Lists.transform(druid.getSegmentPathsToLoad(dataSource,
        new Interval(intervalStr), storageDir, overlordUrl),
        new Function<String, InputSplit>() {
      @Override
      public InputSplit apply(String input)
      {
        return new DruidInputSplit(input);
      }
    });
    
    logger.info("Number of splits = " + splits.size());
    return splits;
  }

  @Override
  public RecordReader<DateTime, Map<String,Object>> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException
  {
    return new DruidRecordReader();
  }
  
  //--- Job Configuration convenience methods ---
  public static void setDataSource(String dataSource, Job job)
  {
    job.getConfiguration().set(CONF_DRUID_DATASOURCE, dataSource);
  }

  public static void setInterval(String interval, Job job)
  {
    job.getConfiguration().set(CONF_DRUID_INTERVAL, interval);
  }

  public static void setSchemaFile(String schemaFile, Job job)
  {
    job.getConfiguration().set(CONF_DRUID_SCHEMA_FILE, schemaFile);
  }

  public static void setSchema(String schema, Job job)
  {
    job.getConfiguration().set(CONF_DRUID_SCHEMA, schema);
  }
}
