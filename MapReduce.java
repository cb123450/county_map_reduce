import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MapReduce {
  
    public static void main( String[] args ) throws Exception {
	Path covid = new Path(args[0]);
	Path output = new Path(args[1]);
	main(new Configuration(), covid, output);
    }

    public static void main(Configuration conf, Path covid, Path output) 
	throws Exception {
	
	runFirstJob(covid, output, new Configuration(conf));
    }

    protected static void runFirstJob(Path covid, Path output, Configuration conf) 
	throws Exception {
	Job job = new Job(conf);
	job.setJarByClass(MapReduce.class);
	job.setJobName("MapReduce Step 1");
	
	
	
	
	
	job.setReducerClass(StateReducer.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setOutputFormatClass(SequenceFileOutputFormat.class);

	MultipleInputs.addInputPath(job, covid, TextInputFormat.class, CovidMapper.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	FileOutputFormat.setOutputPath(job, output);
    
	if (job.waitForCompletion(true)) return;
	else throw new Exception("First Job Failed");
    }

}
