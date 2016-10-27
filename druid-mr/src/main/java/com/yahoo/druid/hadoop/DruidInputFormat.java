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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Ordering;
import io.druid.data.input.InputRow;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.indexer.hadoop.DatasourceInputFormat;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.PartitionChunk;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

//TODO: DatasourceInputSplit.getLocations() returns empty array which might mess up Pig loader
// test that it works w/o that returning new String[]{"xxx"}

/**
 * Hadoop InputFormat to read data from Druid stored on hdfs.
 * <br/>
 * You have to provide following in the job configuration.
 * <br/>
 * <ul>
 * <li>druid.overlord.hostport - overlord host:port</li>
 * <li>druid.datasource.schema - json string containing dataSource, interval, metrics , dimensions etc</li>
 * </ul>
 * <br/>
 * For json schema details, see {@link DatasourceIngestionSpec}
 *
 * For example see src/test/..DruidInputFormatTest.java
 */
public class DruidInputFormat extends DatasourceInputFormat
{

  private static final Logger logger = LoggerFactory.getLogger(DruidInputFormat.class);

  public static final String CONF_DRUID_OVERLORD_HOSTPORT = "druid.overlord.hostport";

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    Configuration conf = context.getConfiguration();

    String overlordUrl = conf.get(CONF_DRUID_OVERLORD_HOSTPORT);
    Preconditions.checkArgument(
        overlordUrl != null && !overlordUrl.isEmpty(),
        CONF_DRUID_OVERLORD_HOSTPORT + " not defined"
    );
    logger.info("druid overlord url = " + overlordUrl);

    String schemaStr = conf.get(CONF_DRUID_SCHEMA);
    Preconditions.checkArgument(
        schemaStr != null && !schemaStr.isEmpty(),
        "schema undefined,  provide " + CONF_DRUID_SCHEMA
    );
    logger.info("schema = " + schemaStr);

    DatasourceIngestionSpec ingestionSpec = HadoopDruidIndexerConfig.jsonMapper.readValue(
        schemaStr,
        DatasourceIngestionSpec.class
    );
    String segmentsStr = getSegmentsToLoad(
        ingestionSpec.getDataSource(),
        ingestionSpec.getInterval(),
        overlordUrl
    );
    logger.info("segments list received from overlord = [%s]", segmentsStr);

    List<DataSegment> segmentsList = HadoopDruidIndexerConfig.jsonMapper.readValue(
        segmentsStr,
        new TypeReference<List<DataSegment>>()
        {
        }
    );
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<>(Ordering.natural());
    for (DataSegment segment : segmentsList) {
      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
    }
    final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(ingestionSpec.getInterval());
    final List<WindowedDataSegment> windowedSegments = new ArrayList<>();
    for (TimelineObjectHolder<String, DataSegment> holder : timeLineSegments) {
      for (PartitionChunk<DataSegment> chunk : holder.getObject()) {
        windowedSegments.add(new WindowedDataSegment(chunk.getObject(), holder.getInterval()));
      }
    }

    conf.set(CONF_INPUT_SEGMENTS, HadoopDruidIndexerConfig.jsonMapper.writeValueAsString(windowedSegments));

    return super.getSplits(context);
  }

  //TODO: change it so that it could use @Global HttpClient injected via Druid
  private String getSegmentsToLoad(String dataSource, Interval interval, String overlordUrl)
  {
    String urlStr = "http://" + overlordUrl + "/druid/indexer/v1/action";
    logger.info("Sending request to overlord at " + urlStr);

    String requestJson = getSegmentListUsedActionJson(dataSource, interval.toString());
    logger.info("request json is " + requestJson);

    int numTries = 3;
    for (int trial = 0; trial < numTries; trial++) {
      try {
        logger.info("attempt number {} to get list of segments from overlord", trial);
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("content-type", "application/json");
        conn.setRequestProperty("Accept", "*/*");
        conn.setUseCaches(false);
        conn.setDoOutput(true);
        conn.setConnectTimeout(60000);
        OutputStream out = conn.getOutputStream();
        out.write(requestJson.getBytes());
        out.close();
        int responseCode = conn.getResponseCode();
        if (responseCode == 200) {
          return IOUtils.toString(conn.getInputStream());
        } else {
          logger.warn(
              "Attempt Failed to get list of segments from overlord. response code [%s] , response [%s]",
              responseCode, IOUtils.toString(conn.getInputStream())
          );
        }
      }
      catch (Exception ex) {
        logger.warn("Exception in getting list of segments from overlord", ex);
      }

      try {
        Thread.sleep(5000); //wait before next trial
      }
      catch (InterruptedException ex) {
        Throwables.propagate(ex);
      }
    }

    throw new RuntimeException(
        String.format(
            "failed to find list of segments, dataSource[%s], interval[%s], overlord[%s]",
            dataSource,
            interval,
            overlordUrl
        )
    );
  }

  protected String getSegmentListUsedActionJson(String dataSource, String interval)
  {
    return "{\"task\": { \"type\" : \"noop\" }," +
           "\"action\": {" +
           "\"type\": \"segmentListUsed\"," +
           "\"dataSource\": \"" + dataSource + "\"," +
           "\"interval\": \"" + interval + "\"" +
           "}}";
  }
  @Override
  public RecordReader<NullWritable, InputRow> createRecordReader(InputSplit split, TaskAttemptContext context)
		  throws IOException, InterruptedException
  {
	return new DruidRecordReader();
  }
}
