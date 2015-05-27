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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.filter.DimFilter;
import io.druid.segment.IndexIO;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import org.apache.commons.io.FileUtils;
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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class DruidRecordReader extends RecordReader<DateTime, Map<String,Object>>
{

  private static final Logger logger = LoggerFactory.getLogger(DruidRecordReader.class);

  private IngestSegmentFirehose rowYielder;
  
  private MapBasedRow currRow;

  private File segmentDir;

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
    
    segmentDir = Files.createTempDir();
    logger.info("segment dir: " + segmentDir);
    
    FileSystem fs = FileSystem.get(context.getConfiguration());
    getSegmentFiles(hdfsPath, segmentDir, fs);
    logger.info("finished getting segment files");

    QueryableIndex index = IndexIO.loadIndex(segmentDir);
    StorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    List<StorageAdapter> adapters = Lists.newArrayList(adapter);
    rowYielder = new IngestSegmentFirehose(adapters, dimensions, metrics, filter, interval, QueryGranularity.NONE);
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException
  {
    if (rowYielder.hasMore()) {
      currRow = (MapBasedRow)rowYielder.nextRow();
      return true;
    } else {
      return false;
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
    if(segmentDir != null) {
      FileUtils.deleteDirectory(segmentDir);
    }
  }
  
  private void getSegmentFiles(String pathStr, File dir, FileSystem fs) throws IOException
  {
    if(!dir.exists() && !dir.mkdirs()) {
      throw new IOException(dir + " does not exist and creation failed");
    }

    final File tmpDownloadFile = File.createTempFile("dataSegment", ".zip");
    if (tmpDownloadFile.exists() && !tmpDownloadFile.delete()) {
      logger.warn("Couldn't clear out temporary file [%s]", tmpDownloadFile);
    }

    try {
      final Path inPath = new Path(pathStr);
      fs.copyToLocalFile(false, inPath, new Path(tmpDownloadFile.toURI()));

      long size = 0L;
      try (final ZipInputStream zipInputStream = new ZipInputStream(
          new BufferedInputStream(
              new FileInputStream(
                  tmpDownloadFile
              )
          )
      )) {
        final byte[] buffer = new byte[1 << 13];
        for (ZipEntry entry = zipInputStream.getNextEntry(); entry != null; entry = zipInputStream.getNextEntry()) {
          final String fileName = entry.getName();
          try (final FileOutputStream fos = new FileOutputStream(
              dir.getAbsolutePath()
              + File.separator
              + fileName
          )) {
            for (int len = zipInputStream.read(buffer); len >= 0; len = zipInputStream.read(buffer)) {
              size += len;
              fos.write(buffer, 0, len);
            }
          }
        }
      }
    }
    finally {
      if (tmpDownloadFile.exists() && !tmpDownloadFile.delete()) {
        logger.warn("Temporary download file could not be deleted [%s]", tmpDownloadFile);
      }
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
