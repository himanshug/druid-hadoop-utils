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
package com.yahoo.druid.hadoop.example;

import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

public class DruidPrintMapper extends Mapper<DateTime, Map<String, Object>, DateTime, Map<String, Object>>
{
  @Override
  public void map(DateTime key, Map<String, Object> value, Context context) {
    System.out.print(key + " , ");
    for(Map.Entry<String,Object> e : value.entrySet()) {
      System.out.print(e.getKey()+"="+e.getValue() + " , ");
    }
    System.out.println("");
  }
}
