import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

//input: date,county,state,cases,deaths,_

public class MapCountyToCases extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
  TextTuple outKey = new TextTuple();
  TextTuple outValue = new TextTuple();
  String sortChar = "c";

  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split(",");
    String state = record[2];
    String county = record[1];
    String cases = record[3];
    String key = state + "_" + county;

    String date = record[0];

    if (date.equals("2020-12-31")) {
	outKey.set(key, sortChar);
	outValue.set("Cases", cases);
	//System.out.println("key: "+outKey+" val: "+outValue);
	context.write(outKey, outValue);
    }
  }

}
