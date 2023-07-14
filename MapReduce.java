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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class MapReduce {
  
    public static void main( String[] args ) throws Exception {
	Path stateCand = new Path(args[0]);
	Path totalVote = new Path(args[1]);
	String outputTempDir = "tempOutput";
	Path output = new Path(args[2]);
	Path temp = new Path(outputTempDir);
	main(new Configuration(), stateCand, totalVote, output, temp);
    }

    public static void main(Configuration conf, Path stateCand, Path totalVote, Path output, Path outputTempDir) 
	throws Exception {
	
	runFirstJob(stateCand, outputTempDir, new Configuration(conf));
	secondFirstJob(outputTempDir, totalVote, output, new Configuration(conf));
    }

    protected static void runFirstJob(Path stateCand, Path output, Configuration conf) 
	throws Exception {
	Job job = new Job(conf);
	job.setJarByClass(MapReduce.class);
	job.setJobName("MapReduce Step 1");
	
	
	FileInputFormat.addInputPath(job, stateCand);
    	job.setInputFormatClass(TextInputFormat.class);
	
	job.setMapperClass(MapLineToStateCandVotes.class);
	job.setReducerClass(ReduceCountyStateVotes.class);
	
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	FileOutputFormat.setOutputPath(job, output);
    
	if (job.waitForCompletion(true)) return;
	else throw new Exception("First Job Failed");
    }

    protected static void secondFirstJob(Path outputTempDir, Path totalVote, Path output, Configuration conf) 
        throws Exception {
        Job job = new Job(conf);
        job.setJarByClass(MapReduce.class);
        job.setJobName("MapReduce Step 2");

	job.setPartitionerClass(SecondarySort.SSPartitioner.class);
	job.setGroupingComparatorClass(SecondarySort.SSGroupComparator.class);
	job.setSortComparatorClass(SecondarySort.SSSortComparator.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setReducerClass(ReducePercentage.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

	MultipleInputs.addInputPath(job, outputTempDir, TextInputFormat.class, MapStateToCandVote.class);
	MultipleInputs.addInputPath(job, totalVote, TextInputFormat.class, MapTotalVotes.class);
	
        job.setMapOutputKeyClass(TextTuple.class);
        job.setMapOutputValueClass(TextTuple.class);
        FileOutputFormat.setOutputPath(job, output);
    
        if (job.waitForCompletion(true)) return;
        else throw new Exception("Second Job Failed");
    }
}
