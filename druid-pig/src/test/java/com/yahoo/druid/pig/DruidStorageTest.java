package com.yahoo.druid.pig;

import com.yahoo.druid.hadoop.OverlordTestServer;
import org.apache.pig.pigunit.PigTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class DruidStorageTest
{
  private PigTest test;

  private static OverlordTestServer server;
  private static int overlordTestPort;

  @BeforeClass
  public static void setUpClass() throws Exception
  {
    server = new OverlordTestServer();
    server.start();
    overlordTestPort = server.getPort();
  }

  @AfterClass
  public static void tearDownClass() throws Exception
  {
    server.stop();
  }

  @Test
  public void testDataInFile() throws Exception
  {
    test = new PigTest("/home/himanshu/work/druid-hadoop-utils/druid-pig/src/test/resources/sample.pig", new String[]{"port=" + overlordTestPort});

    String[] output = { "(450)" };
    test.assertOutput("C", output);
  }
}