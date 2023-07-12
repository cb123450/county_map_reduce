
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

// the first value is location
// if it's not, we don't have a user record, so we'll 
// record the location as UNKNOWN
public class StateReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    Text state = new Text();
    IntWritable deathCases = new IntWritable(0);
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws java.io.IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value: values) {
	  sum += value.get();
      }
      deathCases.set(sum);
      context.write(key, deathCases);
    }

}
