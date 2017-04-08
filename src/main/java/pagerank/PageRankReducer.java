package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableUtils;


public class PageRankReducer extends Reducer<IntWritable,Node,NullWritable, Node>{
	public void reduce(IntWritable key, Iterable<Node> values,Context context)throws IOException, InterruptedException{
		double pageRank = 0.0;
		Node newNode = new Node();
		newNode.setPageNode(key.get());
		for(Node n:values){
			if (n.getType()==Node.IN)
				pageRank += n.getPageRank();
			else
				newNode = (Node)WritableUtils.clone(n, context.getConfiguration());
		}
		Configuration conf = context.getConfiguration();
		long N = conf.getLong("page number",0);
		newNode.setPageRank(pageRank*0.8+0.2*1/N);
		context.write(NullWritable.get(),newNode);
	}

}