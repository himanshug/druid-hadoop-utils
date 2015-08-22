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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.hadoop.DatasourceIngestionSpec;
import io.druid.query.filter.DimFilter;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.List;

public class PigSegmentLoadSpec
{
  private final List<String> dimensions;
  private final List<Metric> metrics;
  private final QueryGranularity granularity;
  private final DimFilter filter;

  @JsonCreator
  public PigSegmentLoadSpec(
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<Metric> metrics,
      @JsonProperty("granularity") QueryGranularity granularity,
      @JsonProperty("filter") DimFilter filter)
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "null dimensions");
    this.metrics = Preconditions.checkNotNull(metrics, "null metrics");
    this.granularity = granularity == null ? QueryGranularity.NONE : granularity;
    this.filter = filter;
  }

  @JsonProperty
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<Metric> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public DimFilter getFilter()
  {
    return filter;
  }
  
  public DatasourceIngestionSpec toDatasourceIngestionSpec(String dataSource, Interval interval) {
    List<String> metricStrs = new ArrayList<>(metrics.size());
    for(Metric m : metrics) {
      metricStrs.add(m.getName());
    }

    return new DatasourceIngestionSpec(
        dataSource,
        interval,
        filter,
        granularity,
        dimensions,
        metricStrs
    );
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
