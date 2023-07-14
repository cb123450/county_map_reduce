import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class ReduceCovidAndVotes extends Reducer<TextTuple, TextTuple, Text, Text> {
    Text state = new Text();
    Text voteInfoAndCases = new Text();
    String line = "";
    
    @Override
    public void reduce(TextTuple key, Iterable<TextTuple> values, Context context) throws java.io.IOException, InterruptedException {
      for (TextTuple value: values) {
	  if (value.left.toString().equals("Voting Info")) {
	      line += value.right.toString() + ",";
	      continue;
	  } else {
	      line += value.right.toString();
	      state = key.left;
	      voteInfoAndCases.set(line);
	      context.write(state, voteInfoAndCases);
	      line = "";
	  }

      }
      //System.out.println("key: "+state+" val: "+state_votes);
    }

}
