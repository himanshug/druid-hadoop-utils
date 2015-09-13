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
package com.yahoo.druid.pig.udfs;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class PostAggregatorAdapter<T> extends EvalFunc<T>
{
  private static final Log LOG = LogFactory.getLog(PostAggregatorAdapter.class);

  private static final TupleFactory tupleFactory = TupleFactory.getInstance();

  protected final PostAggregator aggFactory;
  protected final List<Metric> columnTypes;

  public PostAggregatorAdapter(String aggFactorySpec, String inputSchema) {
    ObjectMapper jsonMapper = HadoopDruidIndexerConfig.jsonMapper;
    try {
      this.aggFactory = jsonMapper.readValue(aggFactorySpec, PostAggregator.class);
      this.columnTypes = jsonMapper.readValue(
          inputSchema, new TypeReference<List<Metric>>()
          {
          }
      );
    } catch(IOException ex) {
      throw new IllegalArgumentException("failed to create aggregator factory", ex);
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    return null;
  }

  @Override
  public T exec(Tuple input) throws IOException {
    try {
      if (input == null || input.size() < 1) {
        throw new IOException("Null Input or Not enough arguments.");
      }

      Map<String, Object> row = new HashMap<>(columnTypes.size());
      for (int i = 0; i < columnTypes.size(); i++) {
        String type = columnTypes.get(i).getType();
        if (DruidUtils.isComplex(type)) {
          ComplexMetricSerde cms = ComplexMetrics.getSerdeForType(type);
          if(cms != null) {
            ObjectStrategy strategy = cms.getObjectStrategy();
            byte[] data = ((DataByteArray)input.get(i)).get();
            row.put(columnTypes.get(i).getName(), strategy.fromByteBuffer(ByteBuffer.wrap(data), data.length));
          } else
            throw new IllegalArgumentException("failed to find object strategy for " + type);
        } else {
          row.put(columnTypes.get(i).getName(), input.get(i));
        }
      }


      //As pig needs a non-Object return type, or else we get following error on foreach
      //ERROR 2080: Foreach currently does not handle type Unknown
      //so we let concrete classes decide whether to return byte[] or finalize the
      //computation on complex object and return Long, Float etc.
      return (T)aggFactory.compute(row);
    } catch (ExecException e) {
      throw new IOException(e);
    }
  }
}

class Metric {
  private String name;
  private String type;

  @JsonCreator
  public Metric(@JsonProperty("name") String name,
                @JsonProperty("type") String type) {
    this.name = name;
    this.type = type;
  }

  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getType()
  {
    return type;
  }
}