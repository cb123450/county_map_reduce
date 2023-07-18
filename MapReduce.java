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
	Path covid = new Path(args[0]);
	Path countyCand = new Path(args[1]);
	Path totalVote = new Path(args[2]);
	Path output = new Path(args[3]);
	
	main(new Configuration(), covid, countyCand, totalVote, output);
    }

    public static void main(Configuration conf, Path covid, Path countyCand, Path totalVote, Path output) 
	throws Exception {
	
	totalJob(covid, countyCand, totalVote, output, new Configuration(conf));
    }
    
    protected static void totalJob(Path covid, Path countyCand, Path totalVote, Path output, Configuration conf) 
        throws Exception {
        Job job = new Job(conf);
        job.setJarByClass(MapReduce.class);
        job.setJobName("Final Job");

	job.setPartitionerClass(SecondarySort.SSPartitioner.class);
	job.setGroupingComparatorClass(SecondarySort.SSGroupComparator.class);
	job.setSortComparatorClass(SecondarySort.SSSortComparator.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setReducerClass(ReducePercentage.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

	MultipleInputs.addInputPath(job, covid, TextInputFormat.class, MapCountyToCases.class);
	MultipleInputs.addInputPath(job, countyCand, TextInputFormat.class, MapCountyToVotes.class);
	MultipleInputs.addInputPath(job, totalVote, TextInputFormat.class, MapTotalVotes.class);

        job.setMapOutputKeyClass(TextTuple.class);
        job.setMapOutputValueClass(TextTuple.class);
        FileOutputFormat.setOutputPath(job, output);
    
        if (job.waitForCompletion(true)) return;
        else throw new Exception("Final Job Failed");
    }
}
