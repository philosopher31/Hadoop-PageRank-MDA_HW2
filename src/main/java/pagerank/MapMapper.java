package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapMapper extends Mapper<LongWritable,Text,IntWritable, IntWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String value_str = value.toString();
        if(value_str.charAt(0)!='#'){
        	String[] tokens = value_str.split("\\s+");
	        IntWritable pageNode = new IntWritable(Integer.valueOf(tokens[0]));
	        IntWritable outlink = new IntWritable(Integer.valueOf(tokens[1]));
	        context.write(pageNode,outlink);
        }
        
    }

}