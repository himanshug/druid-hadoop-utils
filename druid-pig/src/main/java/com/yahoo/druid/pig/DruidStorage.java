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
package com.yahoo.druid.pig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.druid.hadoop.DruidInitialization;
import com.yahoo.druid.hadoop.DruidInputFormat;
import io.druid.data.input.InputRow;
import io.druid.indexer.hadoop.DatasourceRecordReader;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.pig.Expression;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.UDFContext;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * TODO: fix doc if broken
 * A Pig loader to read data from druid segments stored on HDFS.
 * <br/>
 * How to Use?
 * <br/>
 * A = load '&lt;dataSource&gt;' using com.yahoo.druid.pig.DruidStorage('/hdfs/path/to/schema', '&lt;interval&gt;'); 
 * <br/>
 * Schema file is a json containing metrics and dimensions. See {@link PigSegmentLoadSpec}
 *  <br/>
 * Also, set following in the job configuration.
 * <ul>
 * <li>druid.overlord.hostport</li>
 * <li>druid.storage.storageDirectory</li>
 * </ul>
 */
public class DruidStorage extends LoadFunc implements LoadMetadata
{

  private static final Logger logger = LoggerFactory.getLogger(DruidStorage.class);

  private final static String SCHEMA_FILE_KEY = "schemaLocation";
  private final static String INTERVAL_KEY = "interval";
  private final static String SPECS_KEY = "specs";
  private String signature;

  private String dataSource;
  private String schemaFile;
  private String interval;

  private PigSegmentLoadSpec spec;

  private DatasourceRecordReader reader;

  private ObjectMapper jsonMapper;

  public DruidStorage()
  {
    this(null, null);
  }

  public DruidStorage(String schemaFile, String interval)
  {
    this.schemaFile = schemaFile;
    this.interval = interval;

    this.jsonMapper = DruidInitialization.getInstance().getObjectMapper();
  }

  @Override
  public InputFormat<NullWritable, InputRow> getInputFormat() throws IOException
  {
    return new DruidInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException
  {
    try {
      boolean notDone = reader.nextKeyValue();
      if (!notDone) {
        return null;
      }

      InputRow row = reader.getCurrentValue();

      int len = 1 + spec.getDimensions().size() + spec.getMetrics().size();
      Tuple t = TupleFactory.getInstance().newTuple(len);
      t.set(0, row.getTimestamp());

      int i = 1;
      for (String s : spec.getDimensions()) {
        //TODO: will this be enough for multi-valued dimensions?
        t.set(i, row.getDimension(s));
        i++;
      }
      for (Metric m : spec.getMetrics()) {
        if (m.getType().equals("float") || m.getType().equals("long")) {
          t.set(i, row.getRaw(m.getName()));
        } else {
          Object v = row.getRaw(m.getName());
          if (v != null) {
            ComplexMetricSerde cms = ComplexMetrics.getSerdeForType(m.getType());
            if (cms != null) {
              DataByteArray b = new DataByteArray();
              b.set(cms.toBytes(v));
              t.set(i, b);
            } else {
              throw new IOException("Failed to find complex metric serde for " + m.getType());
            }
          } else {
            t.set(i, null);
          }
        }
        i++;
      }

      return t;
    }
    catch (InterruptedException ex) {
      throw new IOException("Failed to read tuples from reader", ex);
    }
  }

  @Override
  public void prepareToRead(RecordReader reader, PigSplit pigSplit) throws IOException
  {
    this.reader = (DatasourceRecordReader) reader;
  }


  //This is required or else impl in LoadFunc will prepend data source string
  //with hdfs://.....
  @Override
  public String relativeToAbsolutePath(String location, Path curDir)
      throws IOException
  {
    return location;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException
  {
    dataSource = location;

    Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass());
    if (UDFContext.getUDFContext().isFrontend()) {
      p.setProperty(signature + SCHEMA_FILE_KEY, schemaFile);
      p.setProperty(signature + INTERVAL_KEY, interval);
      if (spec == null) {
        this.spec = readPigSegmentLoadSpecFromFile(schemaFile, job);
        UDFContext.getUDFContext().getUDFProperties(this.getClass()).setProperty(
            signature + SPECS_KEY,
            jsonMapper.writeValueAsString(spec)
        );
      }
    } else {
      schemaFile = p.getProperty(signature + SCHEMA_FILE_KEY);
      interval = p.getProperty(signature + INTERVAL_KEY);
      spec = jsonMapper.readValue(p.getProperty(signature + SPECS_KEY), PigSegmentLoadSpec.class);
    }

    Configuration conf = job.getConfiguration();
    conf.set(
        DruidInputFormat.CONF_DRUID_SCHEMA,
        jsonMapper.writeValueAsString(spec.toDatasourceIngestionSpec(dataSource, new Interval(interval)))
    );
  }

  @Override
  public void setUDFContextSignature(String signature)
  {
    this.signature = signature;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException
  {
    if (spec == null) {
      this.spec = readPigSegmentLoadSpecFromFile(schemaFile, job);
      UDFContext.getUDFContext().getUDFProperties(this.getClass()).setProperty(
          signature + SPECS_KEY,
          jsonMapper.writeValueAsString(spec)
      );
    }

    int len = 1 + spec.getDimensions().size() + spec.getMetrics().size();
    ResourceFieldSchema[] fields = new ResourceFieldSchema[len];

    fields[0] = new ResourceFieldSchema();
    fields[0].setName("druid_timestamp");
    fields[0].setType(DataType.CHARARRAY);

    int i = 1;
    for (String s : spec.getDimensions()) {
      fields[i] = new ResourceFieldSchema();
      fields[i].setName(s);
      fields[i].setType(DataType.CHARARRAY);
      i++;
    }
    for (Metric m : spec.getMetrics()) {
      fields[i] = new ResourceFieldSchema();
      fields[i].setName(m.getName());

      if (m.getType().equals("float")) {
        fields[i].setType(DataType.FLOAT);
      } else if (m.getType().equals("long")) {
        fields[i].setType(DataType.LONG);
      } else {
        fields[i].setType(DataType.BYTEARRAY);
      }
      i++;
    }

    ResourceSchema schema = new ResourceSchema();
    schema.setFields(fields);
    return schema;
  }

  private PigSegmentLoadSpec readPigSegmentLoadSpecFromFile(
      String schemaFile,
      Job job
  ) throws IOException
  {
    FileSystem fs = FileSystem.get(job.getConfiguration());
    FSDataInputStream in = fs.open(new Path(schemaFile));
    PigSegmentLoadSpec spec = jsonMapper.readValue(
        in.getWrappedStream(),
        PigSegmentLoadSpec.class
    );
    in.close();
    return spec;
  }

  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException
  {
    return null;
  }

  @Override
  public String[] getPartitionKeys(String location, Job job) throws IOException
  {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException
  {
  }
}
