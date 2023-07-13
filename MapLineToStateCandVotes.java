import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// a line contains these fields:
// state,county,candidate,party,total_votes,won
// data is comma delimited

public class CovidMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  Text outKey = new Text();
  IntWritable outValue = new IntWritable(0);
  
  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split(",");
    String state = record[0];
    String candidate = record[2];
    int county_votes = Integer.parseInt(record[4]);
    String state_candidate = state + '_' + candidate;
    outKey.set(state_candidate);
    outValue.set(county_votes);
    // System.out.println("key: "+outKey+" val: "+outValue);
    context.write(outKey, outValue);
  }

}
