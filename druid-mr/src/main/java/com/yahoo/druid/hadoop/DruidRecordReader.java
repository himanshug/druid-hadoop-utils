package com.yahoo.druid.hadoop;

import io.druid.indexer.hadoop.DatasourceRecordReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class DruidRecordReader extends DatasourceRecordReader implements Writable
{
	public DruidRecordReader()
	{
		super();
	}

	@Override
	public void write(DataOutput out) throws IOException
	{
		throw new UnsupportedOperationException();
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		throw new UnsupportedOperationException();
	}
}
