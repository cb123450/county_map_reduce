import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// a line contains these fields:
// state,county,candidate,party,total_votes,won
// data is comma delimited

public class MapStateToCandVote extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
  TextTuple outKey = new TextTuple();
  TextTuple outValue = new TextTuple();
  String sortChar = "b";

  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split("_");
    String state = record[0];
    String[] candAndVotes = record[1].split("\t");
    String candidate = candAndVotes[0];
    String candVotes = candAndVotes[1];
    outKey.set(state, sortChar);
    outValue.set(candidate, candVotes);
    //System.out.println("key: "+outKey+" val: "+outValue);
    context.write(outKey, outValue);
  }

}
