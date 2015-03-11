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

import io.druid.guice.GuiceInjectors;
import io.druid.initialization.DruidModule;

import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Module;

public class DruidInitialization
{

  private static final Logger logger = LoggerFactory.getLogger(DruidInitialization.class);
  
  private ObjectMapper jsonMapper;

  private static final DruidInitialization instance = new DruidInitialization();
  
  private DruidInitialization() {
    ServiceLoader<DruidModule> modules = ServiceLoader.load(
        DruidModule.class, this.getClass().getClassLoader());
    Iterable<Module> iter = (Iterable)modules;
    
    logger.info("Loading Modules....");
    Injector injector = GuiceInjectors.makeStartupInjectorWithModules(iter);

    jsonMapper = injector.getInstance(ObjectMapper.class);
    for(DruidModule m : modules) {
      logger.info("Loading jackson modules for " + m.getClass().getCanonicalName());
      jsonMapper.registerModules((Iterable<com.fasterxml.jackson.databind.Module>) m.getJacksonModules());
    }
  }
  
  public ObjectMapper getObjectMapper() {
    return jsonMapper;
  }

  public static DruidInitialization getInstance() {
    return instance;
  }
}