package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Test {


    // Usage : <input1> : Training set file || <input2> : BloomFilters Input ||  <output> : Test Phase Results


    public static void main(String[] args) throws Exception {

        // Job configuration setup
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Check on the of arguments provided
        if (otherArgs.length != 3) {
            System.err.println("// Usage : <input1> : Training set file || <input2> : BloomFilters Input ||  <output> : Test Phase Results");
            System.exit(1);
        }

        // job: initializes a test phase that outputs for each bloom filter
        // the false negatives, false positives and its probability rate
        Job job = Job.getInstance(conf, "Bloom Filter Testing");
        job.setJarByClass(Test.class);

        // Set up NLineInputFormat which splits N lines of input as one split for the mapper process
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 50000); //50k worked

        // Preserve each BloomFilter byte file in each cluster node
        // in a cached manner for later usage, these files gets deleted at the
        // end of the job from the cache
        for (int i = 1; i <= 10; i++) {
            job.addCacheFile(new Path(otherArgs[1] + "/" + i + "-r-00000").toUri());
        }

        // Setup of job mapper and reducer classes
        job.setMapperClass(BFTestMapper.class);
        job.setReducerClass(BFTestReducer.class);

        // Mapper output (key,value) classes
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Reducer output (key,value) classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the output file of the job
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        // Exit on job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
