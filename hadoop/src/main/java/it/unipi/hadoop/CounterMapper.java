package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This Mapper has the goal of extracting each movie rate, to round it and
 * assign a value of "1" to its value.
 * The map function is called on each line of a movie record that includes its ID
 * and following rating
 */
public  class CounterMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private final Text rating = new Text();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {

        // Split on Movie ID and its rating
        String str = value.toString();
        String[] split = str.split("\\s+");

        // Round the rating value and output the K,V pair
        rating.set(String.valueOf((int) Math.round(Double.parseDouble(split[1]))));
        context.write(rating, one);

    }
}