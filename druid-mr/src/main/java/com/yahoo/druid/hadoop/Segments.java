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

import com.fasterxml.jackson.annotation.JsonProperty;

import io.druid.timeline.DataSegment;

import java.util.List;

/**
 * Created by dayamr on 10/28/16.
 */
public class Segments {

  private final List<DataSegment> segments;

  public Segments(
      @JsonProperty("result") List<DataSegment> segments
  ) {
    this.segments = segments;
  }

  @JsonProperty
  public List<DataSegment> getSegments()
  {
    return segments;
  }
}
