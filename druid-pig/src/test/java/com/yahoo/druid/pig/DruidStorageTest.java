package com.yahoo.druid.pig;

import org.apache.pig.pigunit.PigTest;
import org.junit.Test;

public class DruidStorageTest
{
  private PigTest test;

  @Test
  public void testDataInFile() throws Exception
  {
    test = new PigTest("/home/himanshu/work/druid-hadoop-utils/druid-pig/src/test/resources/sample.pig");

    String[] output = { "(450)" };
    test.assertOutput("C", output);
  }
}