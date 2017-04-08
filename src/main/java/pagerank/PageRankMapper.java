package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable,Text,IntWritable, Node>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String valueStr = value.toString();
        String[] tokens = valueStr.split("\\|");
        int pageNode = Integer.valueOf(tokens[0]);
        double pageRank = Double.valueOf(tokens[1]);
        Configuration conf = context.getConfiguration();
        pageRank += conf.getDouble("rank sum",0);
       	ArrayList<Integer> outlinksArrayList = new ArrayList<Integer>();
        if (tokens.length==3){
	        String[] outlinks = tokens[2].split(",");
	        int outlinksSize = outlinks.length;
	        for(String outlink: outlinks){
	        	IntWritable out = new IntWritable(Integer.valueOf(outlink));
	        	outlinksArrayList.add(Integer.valueOf(outlink));
	        	context.write(out,new Node(pageRank/outlinksSize));
	        }
    	}
        context.write(new IntWritable(pageNode),new Node(pageNode,pageRank,outlinksArrayList));
    }

}