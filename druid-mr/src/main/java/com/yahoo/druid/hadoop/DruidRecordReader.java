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

import io.druid.data.input.MapBasedRow;
import io.druid.query.filter.DimFilter;
import io.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.metamx.common.guava.Yielder;

public class DruidRecordReader extends RecordReader<DateTime, Map<String,Object>>
{

  private static final Logger logger = LoggerFactory.getLogger(DruidRecordReader.class);

  private Yielder<MapBasedRow> rowYielder;
  
  private MapBasedRow currRow;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    ObjectMapper jsonMapper = DruidInitialization.getInstance().getObjectMapper();
    SegmentLoadSpec spec = readSegmentJobSpec(context.getConfiguration(), jsonMapper);
    
    final List<String> dimensions = spec.getDimensions();
    final List<String> metrics = spec.getMetrics();
    final DimFilter filter = spec.getFilter();
    final Interval interval = new Interval(context.getConfiguration().get(DruidInputFormat.CONF_DRUID_INTERVAL));
    
    String hdfsPath = ((DruidInputSplit)split).getPath();
    logger.info("Reading segment from " + hdfsPath);
    
    File segmentDir = Files.createTempDir();
    logger.info("segment dir: " + segmentDir);
    
    FileSystem fs = FileSystem.get(context.getConfiguration());
    getSegmentFiles(hdfsPath, segmentDir, fs);
    logger.info("finished getting segment files");
    
    rowYielder = new DruidHelper().getRowYielder(segmentDir, interval, dimensions, metrics, filter);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if(rowYielder.isDone()) {
      return false;
    } else {
      currRow = rowYielder.get();
      rowYielder = rowYielder.next(currRow);
      return true;
    }
  }

  @Override
  public DateTime getCurrentKey() throws IOException, InterruptedException
  {
    return currRow.getTimestamp();
  }

  @Override
  public Map<String, Object> getCurrentValue() throws IOException, InterruptedException
  {
    return currRow.getEvent();
  }

  @Override
  public float getProgress() throws IOException, InterruptedException
  {
    // TODO can we make this more meaningful?
    return 0;
  }

  @Override
  public void close() throws IOException
  {
    //TODO: may be delete the copied data and delete memory mapping etc
  }
  
  private void getSegmentFiles(String pathStr, File dir, FileSystem fs) throws IOException
  {
    if(!dir.exists() && !dir.mkdirs()) {
      throw new IOException(dir + " does not exist and creation failed");
    }
    final Path path = new Path(pathStr);
    try (FSDataInputStream in = fs.open(path)) {
          CompressionUtils.unzip(in, dir);
    }
  }
  
  public static SegmentLoadSpec readSegmentJobSpec(Configuration config, ObjectMapper jsonMapper) {
    try {
      //first see if schema json itself is present in the config
      String schema = config.get(DruidInputFormat.CONF_DRUID_SCHEMA);
      if(schema != null) {
        logger.info("druid schema  = " + schema);
        return jsonMapper.readValue(schema, SegmentLoadSpec.class);
      }
  
      //then see if schema file location is in the config
      String schemaFile = config.get(DruidInputFormat.CONF_DRUID_SCHEMA_FILE);
      if(schemaFile == null) {
        throw new IllegalStateException("couldn't find schema");
      }
  
      logger.info("druid schema file location = " + schemaFile);

      FileSystem fs = FileSystem.get(config);
      FSDataInputStream in = fs.open(new Path(schemaFile));
      return jsonMapper.readValue(in.getWrappedStream(), SegmentLoadSpec.class);
    } catch(IOException ex) {
      throw new RuntimeException("couldn't load segment load spec", ex);
    }
  }
}
