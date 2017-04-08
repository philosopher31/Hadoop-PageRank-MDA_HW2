package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable,Text,Node, NullWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String valueStr = value.toString();
        String[] tokens = valueStr.split("\\|");
        int pageNode = Integer.valueOf(tokens[0]);
        double pageRank = Double.valueOf(tokens[1]);
        Configuration conf = context.getConfiguration();
        pageRank += conf.getDouble("rank sum",0);
        context.write(new Node(pageNode,pageRank),NullWritable.get());
    }

}