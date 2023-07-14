import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class ReduceCountyStateVotes extends Reducer<Text, IntWritable, Text, IntWritable> {
    Text state = new Text();
    IntWritable state_votes = new IntWritable(0);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
      int tot_votes = 0;
      for (IntWritable value: values) {
	  tot_votes += value.get();
      }
      state_votes.set(tot_votes);
      context.write(key, state_votes);
      //System.out.println("key: "+state+" val: "+state_votes);
    }

}
