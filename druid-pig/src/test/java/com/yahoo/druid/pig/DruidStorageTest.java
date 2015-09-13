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
    test.assertOutput(
        "D",
        new String[]{
            "((2014-10-22T00:00:00.000Z,(,a.example.com)),100,1.0002442201269182)",
            "((2014-10-22T01:00:00.000Z,(,b.example.com)),150,1.0002442201269182)",
            "((2014-10-22T02:00:00.000Z,(,c.example.com)),200,1.0002442201269182)"
        }
    );
  }
}