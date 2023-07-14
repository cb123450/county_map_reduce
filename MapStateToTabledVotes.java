import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

// a line contains these fields:
// state,county,candidate,party,total_votes,won
// data is comma delimited

public class MapStateToTabledVotes extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
  TextTuple outKey = new TextTuple();
  TextTuple outValue = new TextTuple();
  String sortChar = "a";

  @Override  
  public void map(LongWritable key, Text value, Context context) 
  throws java.io.IOException, InterruptedException {
    String[] record = value.toString().split("\t");
    String state = record[0];
    String voteInfo = record[1];
    outKey.set(state, sortChar);
    outValue.set("Voting Info", voteInfo);
    //System.out.println("key: "+outKey+" val: "+outValue);
    context.write(outKey, outValue);
  }

}
