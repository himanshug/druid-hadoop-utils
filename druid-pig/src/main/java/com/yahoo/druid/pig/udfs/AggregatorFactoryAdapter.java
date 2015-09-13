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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Generic UDF to work with any druid complex metric aggregation. 
 */
public abstract class AggregatorFactoryAdapter<T> extends EvalFunc<T>
{
    private static final Log LOG = LogFactory.getLog(AggregatorFactoryAdapter.class);

    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    protected final AggregatorFactory aggFactory;
    protected final ObjectStrategy strategy;

    public AggregatorFactoryAdapter(String aggFactorySpec, String metricType) {
      ObjectMapper jsonMapper = HadoopDruidIndexerConfig.jsonMapper;
      try {
        this.aggFactory = jsonMapper.readValue(aggFactorySpec, AggregatorFactory.class);
      } catch(IOException ex) {
        throw new IllegalArgumentException("failed to create aggregator factory", ex);
      }

      if (DruidUtils.isComplex(metricType)) {
        ComplexMetricSerde cms = ComplexMetrics.getSerdeForType(metricType);
        if (cms != null)
          strategy = cms.getObjectStrategy();
        else
          throw new IllegalArgumentException("failed to find object strategy for " + metricType);
      } else {
        strategy = null;
      }
    }
    @Override
    public Schema outputSchema(Schema input) {
      //TODO: check the input schema?
      //and return the output schema
        return null;
    }

    @Override
    public T exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 1) {
                throw new IOException("Null Input or Not enough arguments.");
            }

            Object obj = input.get(0);
            if (obj == null) {
                return null;
            }

            long n = 0;
            Iterator iter = null;
            if(obj instanceof DataBag) {
              iter = ((DataBag)obj).iterator();
              n = ((DataBag)obj).size();
            } else if(obj instanceof DataByteArray) {
              iter = Lists.newArrayList(tupleFactory.newTuple(obj)).iterator();
              n = 1;
            } else {
              throw new IOException("Unexpected input type " + obj.getClass().getCanonicalName());
            }

            BufferAggregator agg = aggFactory.factorizeBuffered(
                new InternalColumnSelectorFactory(iter, strategy));
            ByteBuffer buff = ByteBuffer.allocate(aggFactory.getMaxIntermediateSize());
            agg.init(buff, 0);
            for(long i = 0; i < n; i++) {
              agg.aggregate(buff, 0);
            }
            
            //As pig needs a non-Object return type, or else we get following error on foreach
            //ERROR 2080: Foreach currently does not handle type Unknown
            //so we let concrete classes decide whether to return byte[] or finalize the
            //computation on complex object and return Long, Float etc.
            return exec(agg, buff);
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }

    protected abstract T exec(BufferAggregator agg, ByteBuffer buff);
}

class InternalColumnSelectorFactory implements ColumnSelectorFactory
{

  private final Iterator inputs;
  private final ObjectStrategy strategy;
  
  public InternalColumnSelectorFactory(Iterator inputs, ObjectStrategy strategy)
  {
    this.inputs = inputs;
    this.strategy = strategy;
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(String paramString)
  {
    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        try {
          Object obj = ((Tuple) inputs.next()).get(0);
          if (obj instanceof Number) {
            return ((Number)obj).longValue();
          } else {
            return Long.parseLong(obj.toString());
          }
        } catch(ExecException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  @Override
  public DimensionSelector makeDimensionSelector(String paramString, ExtractionFn extractionFn)
  {
    throw new RuntimeException("not supported");
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(String paramString)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        try {
          Object obj = ((Tuple) inputs.next()).get(0);
          if (obj instanceof Number) {
            return ((Number)obj).floatValue();
          } else {
            return Float.parseFloat(obj.toString());
          }
        } catch(ExecException ex) {
          throw new RuntimeException(ex);
        }
      }
    };
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(String paramString)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public Object get()
      {
        if(inputs.hasNext()) {
          try {
            DataByteArray d = (DataByteArray)((Tuple)inputs.next()).get(0);
            return strategy.fromByteBuffer(ByteBuffer.wrap(d.get()), d.get().length);
          } catch (ExecException ex) {
            throw new RuntimeException("failed to get to sketch object", ex);
          }
        }
        return null;
      }
    };
  }
}
