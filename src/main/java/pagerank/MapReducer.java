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


public class MapReducer extends Reducer<IntWritable,IntWritable,NullWritable, Node>{
	public void reduce(IntWritable key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException{
		ArrayList<Integer> outlinks = new ArrayList<Integer>();
		for(IntWritable outlink:values){
			outlinks.add(outlink.get());
		}
		Configuration conf = context.getConfiguration();
		long N = conf.getLong("page number",0);
		context.write(NullWritable.get(),new Node(key.get(),1.0/N,outlinks));
	}

}