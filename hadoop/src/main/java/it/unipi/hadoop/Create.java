package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;

public class Create {



    // Usage : <input1> : Dataset file || <input2> : FP Probability Rate ||  <output> : BloomFilter

    public static void main(String[] args) throws Exception {

        // Jobs configuration setup
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // check on the of arguments provided
        if (otherArgs.length != 3) {
            System.err.println("Usage : <input1> : Dataset file || <input2> : FP Probability Rate ||  <output> : BloomFilter");
            System.exit(1);
        }



        // job0: counts the number of movies per rating (rounded rating)
        Job job0 = Job.getInstance(conf, "Movie count per rating");
        job0.setJarByClass(Create.class);

        // Set up NLineInputFormat which splits N lines of input as one split
        // for the mapper process (4 splits in this case = 4 mappers)

        job0.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job0, new Path(otherArgs[0]));
        job0.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 312500);

        // job0: Point to the mapper, reducer and combiner classes
        job0.setMapperClass(CounterMapper.class);
        job0.setCombinerClass(CounterReducer.class);
        job0.setReducerClass(CounterReducer.class);

        // job0 : Set reducer key,value output class
        job0.setOutputKeyClass(Text.class);
        job0.setOutputValueClass(IntWritable.class);

        // job0 : Set the output path file
        FileOutputFormat.setOutputPath(job0,
                new Path("ratingCounts"));

        // job0 : wait for the completion in order to start the second one
        job0.waitForCompletion(true);




        Configuration conf2 = new Configuration();

        // set the false positive probability rate for the mapper and reducer
        conf2.set("fpRate", otherArgs[1]);

        // job1 : Create a bloom-filter for each rating with set indexes
        // and with a specific false positive rate value , and then  save each one
        // in a byte array file for later usage in testing phase

        Job job1 = Job.getInstance(conf2, "Bloom Filter Creation");
        job1.setJarByClass(Create.class);



        // Set up NLineInputFormat which splits N lines of input as one split
        // for the mapper process (4 splits in this case = 4 mappers)

        job1.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        job1.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 312500);

        // job1: Point to the mapper, reducer classes
        job1.setMapperClass(BFCreateMapper.class);
        job1.setReducerClass(BFCreateReducer.class);

        //job1: set up the key,value output class for the mapper
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        //job1: set up the key,value output class for reducer
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(BloomFilterWritable.class);

        //job1: we are using multiple outputs, each bloom-filter is output as a byte file in different file
        // here we set up the naming of the output files
        for (int i = 1; i <= 10; i++) {
            MultipleOutputs.addNamedOutput(job1, Integer.toString(i), BloomFilterOutputFormat.class
                    , NullWritable.class, BloomFilterWritable.class);
        }

        //job1: add the rating counts from earlier job to as a cached file to cluster nodes
        job1.addCacheFile(new Path("ratingCounts/part-r-00000").toUri());

        //job1: set the output files path
        FileOutputFormat.setOutputPath(job1,
                new Path(otherArgs[2]));

        //job1 : exit on completion of second job
        System.exit(job1.waitForCompletion(true) ? 0 : 1);


    }
}
