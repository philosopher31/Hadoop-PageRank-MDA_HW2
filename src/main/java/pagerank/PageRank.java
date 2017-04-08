package pagerank;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
    static public enum CounterType
    {
        PAGE_COUNTER;
    }

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Page Rank <in> <out> <iterate times>");
            System.exit(3);
        }
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        int iterateTimes = Integer.valueOf(otherArgs[2]);
        Job countJob = new Job(conf, "Create Map");
        countJob.setJarByClass(PageRank.class);
        countJob.setMapperClass(CountMapper.class);
        countJob.setReducerClass(CountReducer.class);
        countJob.setMapOutputKeyClass(IntWritable.class);
        countJob.setMapOutputValueClass(NullWritable.class);
        countJob.setOutputKeyClass(NullWritable.class);
        countJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(countJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(countJob, new Path(outputPath+"/out0"));
        countJob.waitForCompletion(true);

        long pageNum = countJob.getCounters().findCounter(CounterType.PAGE_COUNTER).getValue();
        conf.setLong("page number", pageNum);
        System.out.println(pageNum);

        Job mapJob = new Job(conf, "Create Map");
        mapJob.setJarByClass(PageRank.class);
        mapJob.setMapperClass(MapMapper.class);
        //job.setCombinerClass(PageRankReducer.class);
        mapJob.setReducerClass(MapReducer.class);
        mapJob.setMapOutputKeyClass(IntWritable.class);
    	mapJob.setMapOutputValueClass(IntWritable.class);
        mapJob.setOutputKeyClass(NullWritable.class);
        mapJob.setOutputValueClass(Node.class);
        FileInputFormat.addInputPath(mapJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(mapJob, new Path(outputPath+"/out1"));
        mapJob.waitForCompletion(true);
        
        int i;
        FileSystem f =FileSystem.get(conf);
        for(i = 1; i <= iterateTimes; i++){
            f.delete(new Path(otherArgs[1]+"/out"+(i+1)),true);
            f.delete(new Path(otherArgs[1]+"/util"+i),true);

            Job rankJob = new Job(conf, "Page Rank");
	        rankJob.setJarByClass(PageRank.class);
	        rankJob.setMapperClass(PageRankMapper.class);
	        rankJob.setReducerClass(PageRankReducer.class);
	        rankJob.setMapOutputKeyClass(IntWritable.class);
	        rankJob.setMapOutputValueClass(Node.class);
	        rankJob.setOutputKeyClass(NullWritable.class);
	        rankJob.setOutputValueClass(Node.class);
	        FileInputFormat.addInputPath(rankJob, new Path(outputPath+"/out"+i));
	        FileOutputFormat.setOutputPath(rankJob, new Path(outputPath+"/out"+(i+1)));
	        rankJob.waitForCompletion(true);

            Job sumJob = new Job(conf, "Page Rank");
            sumJob.setJarByClass(PageRank.class);
            sumJob.setMapperClass(SumMapper.class);
            sumJob.setReducerClass(SumReducer.class);
            sumJob.setMapOutputKeyClass(IntWritable.class);
            sumJob.setMapOutputValueClass(DoubleWritable.class);
            sumJob.setOutputKeyClass(NullWritable.class);
            sumJob.setOutputValueClass(DoubleWritable.class);
            FileInputFormat.addInputPath(sumJob, new Path(outputPath+"/out"+(i+1)));
            FileOutputFormat.setOutputPath(sumJob, new Path(outputPath+"/util"+i));
            sumJob.waitForCompletion(true);
            double rankSum = readRankSum(conf,new Path(outputPath+"/util"+i+"/part-r-00000"));
            conf.setDouble("rank sum",rankSum);
            double oldRankSum = readRankSum(conf,new Path(outputPath+"/util"+(i-1)+"/part-r-00000"));

        }

        if (f.exists(new Path(otherArgs[1] + "/FinalOut"))){
                f.delete(new Path(otherArgs[1] + "/FinalOut"),true);
        }
        Job sortJob = new Job(conf, "Normalize and Sort Page Rank");
        sortJob.setJarByClass(PageRank.class);
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        sortJob.setMapOutputKeyClass(Node.class);
        sortJob.setMapOutputValueClass(NullWritable.class);
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(sortJob, new Path(outputPath + "/out" + i));
        FileOutputFormat.setOutputPath(sortJob, new Path(outputPath + "/FinalOut"));
        sortJob.waitForCompletion(true);

    }
  
    public static double readRankSum(Configuration conf,Path path)
        throws FileNotFoundException, IOException, UnsupportedEncodingException {
        int counter = 0;
        FileSystem fs = FileSystem.get(conf);
        double rankSum = 0.0;
        if(fs.exists(path)){
            FSDataInputStream fsStream = fs.open(path);
            BufferedReader in = new BufferedReader(new InputStreamReader(fsStream,"UTF-8"));
            String line;
            while((line = in.readLine()) != null ){
               rankSum = Double.valueOf(line);
            }
            in.close();
        }
        return rankSum;
    }
	
}
