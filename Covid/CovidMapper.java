import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// a county record contains these fields:
// date,county,state,fips,cases,deaths
// data is comma delimited

public class CovidMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  Text outKey = new Text();
  IntWritable outValue = new IntWritable(0);
  
  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split(",");
    String date = record[0];
    String state = record[2];
    int cases = Integer.parseInt(record[4]);
    if (date.equals("2020-12-31")) {
	    outKey.set(state);
	    outValue.set(cases);
	    // System.out.println("key: "+outKey+" val: "+outValue);
	    context.write(outKey, outValue);
	}
  }

}
