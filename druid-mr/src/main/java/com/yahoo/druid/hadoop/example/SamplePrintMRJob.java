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
package com.yahoo.druid.hadoop.example;


import com.yahoo.druid.hadoop.DruidInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;

import java.util.Map;

public class SamplePrintMRJob extends Configured implements Tool
{
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SamplePrintMRJob(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    // When implementing tool
    Configuration conf = this.getConf();
 
        // Create job
    Job job = new Job(conf, "Druid-Loader-Sample-Job");
    job.setJarByClass(SamplePrintMRJob.class);
//    job.setJobName("Druid-Loader-Sample-Job");

    job.getConfiguration().set("mapreduce.job.acl-view-job", "*");
    job.getConfiguration().set("mapreduce.job.queuename", "default");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Duser.timezone=UTC");
    //job.getConfiguration().set("mapreduce.map.memory.mb", "1024");
    
    
    job.getConfiguration().set(DruidInputFormat.CONF_DRUID_STORAGE_STORAGE_DIR, "/tmp/druid/storage");
    job.getConfiguration().set(DruidInputFormat.CONF_DRUID_OVERLORD_HOSTPORT, "localhost:8080");
    job.getConfiguration().set(DruidInputFormat.CONF_DRUID_DATASOURCE, "wikipedia");
    job.getConfiguration().set(DruidInputFormat.CONF_DRUID_INTERVAL, "2009-01-01T00:00:00.000/2050-01-01T00:00:00.000");
    job.getConfiguration().set(DruidInputFormat.CONF_DRUID_SCHEMA_FILE, "/tmp/druid/schema/druid_fun_mr.json");
    
    job.setMapperClass(DruidPrintMapper.class);
    job.setNumReduceTasks(0);
    
    job.setOutputKeyClass(DateTime.class);
    job.setOutputValueClass(Map.class);

    job.setInputFormatClass(DruidInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);
    
    System.out.println("Starting Druid Loader Sample Job.....");
    return job.waitForCompletion(true) ? 0 : 1;
    //System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
