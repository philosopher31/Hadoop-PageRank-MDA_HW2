package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class SortReducer extends Reducer<Node,NullWritable,IntWritable, DoubleWritable>{
	public void reduce(Node key, Iterable<NullWritable> values,Context context)throws IOException, InterruptedException{
		IntWritable pageNode = new IntWritable(key.getPageNode());
		DoubleWritable pageRank = new DoubleWritable(key.getPageRank());
		context.write(pageNode,pageRank);
	}

}