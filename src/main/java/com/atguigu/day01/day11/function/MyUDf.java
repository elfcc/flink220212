package com.atguigu.day01.day11.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @author cc
 * @create 2022/7/13 001313:59
 * @Version 1.0
 */
public class MyUDf extends ScalarFunction {
   public String ToDa(String word){
       return word.toUpperCase();
   }
}
