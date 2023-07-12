import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

// a county record contains these fields:
// date,county,state,fips,cases,deaths
// data is comma delimited

public class CovidMapper extends Mapper<IntWritable, Text, Text, IntWritable> {
  Text outKey = new Text();
  IntWritable outValue = new IntWritable(0);
  
  @Override  
  public void map(IntWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split(",");
    String state = record[2];
    int cases = Integer.parseInt(record[4]);
    outKey.set(state);
    outValue.set(cases);
    context.write(outKey, outValue);
  }

}
