package pagerank;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SumMapper extends Mapper<LongWritable,Text,IntWritable, DoubleWritable>{
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String valueStr = value.toString();
        String[] tokens = valueStr.split("\\|");
        double pageRank = Double.valueOf(tokens[1]);
        context.write(new IntWritable(1),new DoubleWritable(pageRank));
    }

}