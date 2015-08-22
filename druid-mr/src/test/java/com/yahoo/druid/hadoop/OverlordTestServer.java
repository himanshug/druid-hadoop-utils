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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.BindException;
import java.net.ServerSocket;

/**
 */
public class OverlordTestServer
{
  private static final Logger logger = LoggerFactory.getLogger(OverlordTestServer.class);

  private int port = 4000;
  private Server server;

  public void start() throws Exception
  {
    logger.info("Starting OverlordTestServer................");

    port = findFreePort(port);
    server = new Server(port);
    ServletContextHandler root = new ServletContextHandler(ServletContextHandler.SESSIONS);
    root.addServlet(SegmentListerServlet.class, "/druid/indexer/v1/action");

    HandlerList handlerList = new HandlerList();
    handlerList.setHandlers(new Handler[]{root});
    server.setHandler(handlerList);

    server.start();
    logger.info("Started OverlordTestServer................");
  }


  public void stop() throws Exception
  {
    logger.info("Stopping OverlordTestServer................");
    server.stop();
    logger.info("Stopped OverlordTestServer................");
  }

  public int getPort()
  {
    return port;
  }

  private int findFreePort(int startPort)
  {
    int port = startPort;
    while (true) {
      port++;
      ServerSocket ss = null;
      try {
        ss = new ServerSocket(port);
        return port;
      }
      catch (BindException be) {
        //port in use
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      finally {
        if (ss != null) {
          while (!ss.isClosed()) {
            try {
              ss.close();
            }
            catch (IOException e) {
              // ignore
            }
          }
        }
      }
    }
  }

  public static class SegmentListerServlet extends HttpServlet
  {

    private String responseStr;

    public SegmentListerServlet()
    {
      try {
        ObjectMapper jsonMapper = new DefaultObjectMapper();
        DataSegment segment = jsonMapper
            .readValue(this.getClass().getClassLoader().getResource("test-segment/descriptor.json"), DataSegment.class)
            .withLoadSpec(
                ImmutableMap.<String, Object>of(
                    "type",
                    "local",
                    "path",
                    this.getClass().getClassLoader().getResource("test-segment/index.zip").getPath()
                )
            );
        responseStr = "[" + jsonMapper.writeValueAsString(segment) + "]";
      }
      catch (IOException ex) {
        logger.error("failed to setup resource" + ex.toString(), ex);
        throw new RuntimeException(ex);
      }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
        throws IOException
    {
      PrintWriter out = response.getWriter();
      out.print(responseStr);
    }
  }
}
