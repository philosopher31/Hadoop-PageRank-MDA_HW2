package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import pagerank.PageRank.CounterType;


public class CountReducer extends Reducer<IntWritable,IntWritable,NullWritable, NullWritable>{
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException{
		ArrayList<Integer> outlinks = new ArrayList<Integer>();
		context.getCounter(CounterType.PAGE_COUNTER).increment(1);  // count page number
	}

}