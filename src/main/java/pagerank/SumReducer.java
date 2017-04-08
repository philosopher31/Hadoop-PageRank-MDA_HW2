package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableUtils;


public class SumReducer extends Reducer<IntWritable,DoubleWritable,NullWritable, DoubleWritable>{
	public void reduce(IntWritable key, Iterable<DoubleWritable> values,Context context)throws IOException, InterruptedException{
		double pageRankSum = 0.0;
		for(DoubleWritable d:values){
			pageRankSum+=d.get();
		}
		Configuration conf = context.getConfiguration();
		long N = conf.getLong("page number",0);
		context.write(NullWritable.get(),new DoubleWritable((1-pageRankSum)/N));
	}

}