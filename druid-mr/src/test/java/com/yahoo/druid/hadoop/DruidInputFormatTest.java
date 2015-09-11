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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.druid.hadoop.example.SamplePrintMRJob;
import io.druid.data.input.InputRow;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DruidInputFormatTest
{
  private OverlordTestServer server;
  private int overlordTestPort;

  @BeforeClass
  public void setUpClass() throws Exception
  {
    server = new OverlordTestServer();
    server.start();
    overlordTestPort = server.getPort();
  }

  @AfterClass
  public void tearDownClass() throws Exception
  {
    server.stop();
  }

  @Test
  public void testSampleMRJob() throws Exception
  {
    Job job = Job.getInstance(
        new Configuration(),
        "Druid-Loader-Sample-Test-Job"
    );

    job.getConfiguration().set("mapreduce.job.acl-view-job", "*");
    job.getConfiguration().set("mapreduce.map.java.opts", "-Duser.timezone=UTC");
    job.getConfiguration().set(DruidInputFormat.CONF_DRUID_OVERLORD_HOSTPORT, "localhost:" + overlordTestPort);
    job.getConfiguration().set(
        DruidInputFormat.CONF_DRUID_SCHEMA,
        "{"
        + "\"dataSource\":\"testDataSource\","
        + "\"interval\":\"1970-01-01T00:00:00.000Z/3000-01-01T00:00:00.000Z\","
        + "\"granularity\":\"NONE\","
        + "\"dimensions\":[\"host\"],"
        + "\"metrics\":[\"visited_sum\",\"unique_hosts\"]"
        + "}"
    );

    job.setMapperClass(SampleMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setInputFormatClass(DruidInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class);

    Assert.assertTrue(job.waitForCompletion(true));

    //TODO: somehow verify that the verifyRows in SampleMapper.run() is actually called, may be that
    //creates a file and you verify that indeed that file exists at this point or something like
    //that
  }

  public static class SampleMapper extends Mapper<NullWritable, InputRow, NullWritable, NullWritable>
  {
    @Override
    public void run(Context context) throws IOException, InterruptedException
    {
      setup(context);
      List<InputRow> actuals = new ArrayList<>();
      try {
        while (context.nextKeyValue()) {
          actuals.add(context.getCurrentValue());
        }
      }
      finally {
        cleanup(context);
      }
      verifyRows(actuals);
    }

    private void verifyRows(List<InputRow> actualRows)
    {
      List<ImmutableMap<String, Object>> expectedRows = ImmutableList.of(
          ImmutableMap.<String, Object>of(
              "time", DateTime.parse("2014-10-22T00:00:00.000Z"),
              "host", ImmutableList.of("a.example.com"),
              "visited_sum", 100L,
              "unique_hosts", 1.0d
          ),
          ImmutableMap.<String, Object>of(
              "time", DateTime.parse("2014-10-22T01:00:00.000Z"),
              "host", ImmutableList.of("b.example.com"),
              "visited_sum", 150L,
              "unique_hosts", 1.0d
          ),
          ImmutableMap.<String, Object>of(
              "time", DateTime.parse("2014-10-22T02:00:00.000Z"),
              "host", ImmutableList.of("c.example.com"),
              "visited_sum", 200L,
              "unique_hosts", 1.0d
          )
      );

      Assert.assertEquals(expectedRows.size(), actualRows.size());

      for (int i = 0; i < expectedRows.size(); i++) {
        Map<String, Object> expected = expectedRows.get(i);
        InputRow actual = actualRows.get(i);

        Assert.assertEquals(ImmutableList.of("host"), actual.getDimensions());

        Assert.assertEquals(expected.get("time"), actual.getTimestamp());
        Assert.assertEquals(expected.get("host"), actual.getDimension("host"));
        Assert.assertEquals(expected.get("visited_sum"), actual.getLongMetric("visited_sum"));
        Assert.assertEquals(
            (Double) expected.get("unique_hosts"),
            (Double) HyperUniquesAggregatorFactory.estimateCardinality(actual.getRaw("unique_hosts")),
            0.001
        );
      }
    }
  }
}
