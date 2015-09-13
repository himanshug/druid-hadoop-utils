package com.yahoo.druid.pig.udfs;

public class DoublePostAggregatorAdapter extends PostAggregatorAdapter<Double>
{
  public DoublePostAggregatorAdapter(String aggFactorySpec, String inputSchema)
  {
    super(aggFactorySpec, inputSchema);
  }
}
