package com.yahoo.druid.pig.udfs;

public class FloatPostAggregatorAdapter extends PostAggregatorAdapter<Float>
{
  public FloatPostAggregatorAdapter(String aggFactorySpec, String inputSchema)
  {
    super(aggFactorySpec, inputSchema);
  }
}
