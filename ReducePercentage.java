import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class ReducePercentage extends Reducer<TextTuple, TextTuple, Text, Text> {
    Text state = new Text();
    Text voteInfoAndCases = new Text();
    Double percent = 0.0;
    Double totalV = 0.0;
    String line = "";
    
    @Override
    public void reduce(TextTuple key, Iterable<TextTuple> values, Context context) throws java.io.IOException, InterruptedException {
      for (TextTuple value: values) {
	  if (value.left.toString().equals("Total Votes")) {
	      totalV = Double.valueOf(value.right.toString()); 
	      line += value.right.toString() + ",";
	      continue;
	  } else if(value.left.toString().equals("Cases")) {
	      line += value.right.toString();
              state = key.left;
              voteInfoAndCases.set(line);
              context.write(state, voteInfoAndCases);
              line = "";

	  } else {
	      percent = Double.valueOf(value.right.toString())/totalV;
	      line += value.left.toString() + "-" + percent.toString() + ",";
	  }
      }
      //System.out.println("key: "+state+" val: "+state_votes);
    }

}
