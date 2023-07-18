import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

//input: state,county,candidate,party,votes,win

public class MapLineToCountyCandVotes extends Mapper<LongWritable, Text, TextTuple, TextTuple> {
    TextTuple outKey = new TextTuple();
    TextTuple outValue = new TextTuple();
    String sortChar = "b";
    int otherVotes = 0;
    String prevCounty = "NULL";
    String prevState = "NULL";
    @Override  
    public void map(LongWritable key, Text value, Context context) 
	throws java.io.IOException, InterruptedException {
	
	String[] record = value.toString().split(",");
	String county = record[1];
	String state = record[0];
	
	if (!prevCounty.equals(county) && !prevCounty.equals("NULL")){
	    outKey.set(prevState + "_" + prevCounty, sortChar);
	    outValue.set("Other", otherCount);
	    otherVotes = 0;
	    //System.out.println("key: "+outKey+" val: "+outValue);
	    context.write(outKey, outValue);
	    prevCounty = county;
	    prevState = state;
	}

	String candidate = record[2];
	String county_votes = record[4];
	if (!candidate.equals("Joe Biden") && !candidate.equals("Donald Trump")) {
	    candidate = "Other";
	    otherVotes += Integer.parse(county_votes);
	}
	
	String state_county = state + '_' + county;

	if (!candidate.equals("Other")){
	    outKey.set(state_county, sortChar);
	    outValue.set(candidate, county_votes);
	    //System.out.println("key: "+outKey+" val: "+outValue);
	    context.write(outKey, outValue);
	}

	prevCounty = county;
	prevState = state;

	if (value.toString().contains("Arizona,Mohave County, Write-ins")){
	    outKey.set(state_county, sortChar);
	    outValue.set(candidate, otherVotes);
	    context.write(outKey, outValue);
	}
    }
}
