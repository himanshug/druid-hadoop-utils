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

import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IndexIO;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexStorageAdapter;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TimestampColumnSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;


public class DruidHelper
{
  private static final Logger logger = LoggerFactory.getLogger(DruidHelper.class);

  public List<String> getSegmentPathsToLoad(final String dataSource, final Interval interval,
      final String storageDir, final String overlordUrl) {
    List<DataSegment> segmentsToLoad = getSegmentsToLoad(dataSource, interval, overlordUrl);
    VersionedIntervalTimeline<String, DataSegment> timeline = new VersionedIntervalTimeline<String, DataSegment>(
          Ordering.<String>natural().nullsFirst()
      );
    for (DataSegment segment : segmentsToLoad) {
      timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(segment));
    }
    final List<TimelineObjectHolder<String, DataSegment>> timeLineSegments = timeline.lookup(interval);
    
    return Lists.transform(timeLineSegments, new Function<TimelineObjectHolder<String, DataSegment>, String>() {
      @Override
      public String apply(TimelineObjectHolder<String, DataSegment> input)
      {
        DataSegment segment = input.getObject().getChunk(0).getObject();
        //TODO: make sure that there is no problem if storageDir ends with a / already
        return storageDir + "/" + dataSource + "/" + DataSegmentPusherUtil.getHdfsStorageDir(segment) + "/index.zip";
      }
    });
  }

  public Yielder<MapBasedRow> getRowYielder(final File segmentDir, final Interval interval, final List<String> dimensions,
      final List<String> metrics, final DimFilter filter) throws IOException {
    QueryableIndex index = IndexIO.loadIndex(segmentDir);
    StorageAdapter adapter = new QueryableIndexStorageAdapter(index);
    
    Sequence<MapBasedRow> rows = Sequences.concat(
        Sequences.map(adapter.makeCursors(Filters.convertDimensionFilters(filter), interval, QueryGranularity.ALL),
            new Function<Cursor, Sequence<MapBasedRow>>() {
              @Override
              public Sequence<MapBasedRow> apply(final Cursor cursor)
              {
                final TimestampColumnSelector timestampColumnSelector = cursor.makeTimestampColumnSelector();
                
                final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                for (String dim : dimensions) {
                  final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim);
                  if (dimSelector != null) {
                    dimSelectors.put(dim, dimSelector);
                  } else {
                    logger.warn("dimension " + dim + " not found");
                  }
                }
                
                final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
                for (String metric : metrics) {
                  //Note: this fails silently without any error for complex metrics if relevant
                  //serde is not registered to ComplexMetrics.
                  final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
                  if (metricSelector != null) {
                    metSelectors.put(metric, metricSelector);
                  } else {
                    logger.warn("metric " + metric + " not found");
                  }
                }
                
                return Sequences.simple(
                    new Iterable<MapBasedRow>()
                    {
                      @Override
                      public Iterator<MapBasedRow> iterator()
                      {
                        return new Iterator<MapBasedRow>()
                        {
                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone();
                          }
    
                          @Override
                          public MapBasedRow next()
                          {
                            final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                            final long timestamp = timestampColumnSelector.getTimestamp();
                            theEvent.put(EventHolder.timestampKey, new DateTime(timestamp));

                            for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                              final String dim = dimSelector.getKey();
                              final DimensionSelector selector = dimSelector.getValue();
                              final IndexedInts vals = selector.getRow();
    
                              if (vals.size() == 1) {
                                final String dimVal = selector.lookupName(vals.get(0));
                                theEvent.put(dim, dimVal);
                              } else {
                                List<String> dimVals = Lists.newArrayList();
                                for (int i = 0; i < vals.size(); ++i) {
                                  dimVals.add(selector.lookupName(vals.get(i)));
                                }
                                theEvent.put(dim, dimVals);
                              }
                            }
    
                            for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
                              final String metric = metSelector.getKey();
                              final ObjectColumnSelector selector = metSelector.getValue();
                              theEvent.put(metric, selector.get());
                            }
                            
                            cursor.advance();
                            return new MapBasedInputRow(timestamp, dimensions, theEvent);
                          }
    
                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException("Remove Not Supported");
                          }
                        };
                      }
                    }
                );
              }
            }
        )
    );
    
    return rows.toYielder(
        null,
        new YieldingAccumulator()
        {
          @Override
          public Object accumulate(Object accumulated, Object in)
          {
            yield();
            return in;
          }
        }
      );
  }

  protected List<DataSegment> getSegmentsToLoad(String dataSource, Interval interval, String overlordUrl) {
    String urlStr = "http://" + overlordUrl + "/druid/indexer/v1/action";
    logger.info("Sending request to overlord at " + urlStr);
    
    String requestJson = getSegmentListUsedActionJson(dataSource, interval.toString());
    logger.info("request json is " + requestJson);
    
    int numTries = 3; //TODO: should be configurable?
    for(int trial = 0; trial < numTries; trial++) {
      try {
        logger.info("attempt number {} to get list of segments from overlord", trial);
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection)url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("content-type", "application/json");
        conn.setUseCaches(false);
        conn.setDoOutput(true);
        conn.setConnectTimeout(60000); //TODO: 60 secs, shud be configurable?
        OutputStream out = conn.getOutputStream();
        out.write(requestJson.getBytes());
        out.close();
        int responseCode = conn.getResponseCode();
        if(responseCode == 200) {
          ObjectMapper mapper = new DefaultObjectMapper();
          Map<String,Object> obj = 
            mapper.readValue(conn.getInputStream(), new TypeReference<Map<String,Object>>(){});
          return mapper.convertValue(obj.get("result"), new TypeReference<List<DataSegment>>(){});
        } else {
          logger.warn("Attempt Failed to get list of segments from overlord. response code {} , response {}",
              responseCode, IOUtils.toString(conn.getInputStream()));
        }
      } catch(Exception ex) {
        logger.warn("Exception in getting list of segments from overlord", ex);
      }
      
      try {
        Thread.sleep(5000); //wait before next trial
      } catch(InterruptedException ex) {
        Throwables.propagate(ex);
      }
    }
    
    throw new RuntimeException("failed to find list of segments from overlord");
  }

  protected String getSegmentListUsedActionJson(String dataSource, String interval) {
    return "{\"task\": { \"type\" : \"noop\" }," +
      "\"action\": {" +
         "\"type\": \"segmentListUsed\"," +
         "\"dataSource\": \"" + dataSource+ "\"," +
         "\"interval\": \"" + interval + "\"" +
       "}}";
  }
}
