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

import io.druid.query.filter.DimFilter;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.yahoo.druid.hadoop.SegmentLoadSpec;

public class PigSegmentLoadSpec
{
  private List<String> dimensions;
  private List<Metric> metrics;
  private DimFilter filter;

  @JsonCreator
  public PigSegmentLoadSpec(@JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("metrics") List<Metric> metrics,
      @JsonProperty("filter") DimFilter filter)
  {
    this.dimensions = Preconditions.checkNotNull(dimensions, "null dimensions");
    this.metrics = Preconditions.checkNotNull(metrics, "null metrics");
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
  
  public SegmentLoadSpec toSegmentLoadSpec() {
    List<String> metricStrs = new ArrayList<>(metrics.size());
    for(Metric m : metrics) {
      metricStrs.add(m.getName());
    }
    return new SegmentLoadSpec(dimensions, metricStrs, filter);
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
