package com.yahoo.druid.pig.udfs;

/**
 */
public class DruidUtils
{
  public static boolean isComplex(String type)
  {
    return type.equals("complex") ||
           !(type.equals("float") || !type.equals("long") || type.equals("string") || type.equals("simple"));
  }
}
