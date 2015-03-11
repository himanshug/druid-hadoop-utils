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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class DruidInputSplit extends InputSplit implements Writable
{
  //absolute hdfs path all the way to index.zip
  private String path;
  
  public DruidInputSplit() {}
  
  public DruidInputSplit(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

  @Override
  public long getLength() throws IOException {
    //TODO: may be provide a meaningful value. for now returning large enough value
    //so that pig loader does not try to combine the splits
    return Long.MAX_VALUE;
  }

  @Override
  public String[] getLocations() throws IOException {
    //TODO: this is supposed to be the address of "preferred" hadoop cluster nodes
    //may be returning empty array here should be OK for now..
    //pig does not seem to work if this array is empty.
    //actually we can see where most of the blocks for segment are stored and prefer
    //to run the mapper on those hosts
    return new String[]{"xxx"};
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    Text.writeString(out, path);
  }
  @Override
  public void readFields(DataInput in) throws IOException
  {
    path = Text.readString(in);
  }
  
  @Override
  public String toString() {
    return "[" + this.getClass().getName() + ":" + path + "]";
  }
}
