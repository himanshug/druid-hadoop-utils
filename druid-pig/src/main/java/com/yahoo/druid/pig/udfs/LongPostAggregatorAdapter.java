package com.yahoo.druid.pig.udfs;

public class LongPostAggregatorAdapter extends PostAggregatorAdapter<Long>
{
  public LongPostAggregatorAdapter(String aggFactorySpec, String inputSchema)
  {
    super(aggFactorySpec, inputSchema);
  }
}
