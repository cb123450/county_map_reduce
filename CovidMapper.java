package com.matthewrathbone.example;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

// a county record contains these fields:
// date,county,state,fips,cases,deaths
// data is comma delimited

public class CovidMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
  Text outKey = new Text();
  Text outValue = new LongWritable(0);
  
  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split(",");
    String county = record[1];
    String cases = record[3];
    outKey.set(county);
    outValue.set(cases);
    context.write(outKey, outValue);
  }

}
