package it.unipi.hadoop;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * This Reducer has the goal of summing up the number of received
 * false positives  and false negatives for each bloom-filter and
 * outputting the results in a string format.
 * The result will also include the false positive probability rate
 * for each of 10 bloom-filters
 *
 * A false positive rate : follows the formula : (False positive count) / (True Negatives)
 *
 * Setup() function import the rating count file in which their values are used in the
 * false positive rate formula
 *
 * Reducer() function sets the sum of the FP and FN counts and outputs
 * (K,V) pair where K= rating, V = String of test statistics
 */

public class BFTestReducer
        extends Reducer<Text, IntWritable, Text, Text> {
    int[] numMoviesPerRating = new int[10];
    Text result = new Text();
    int movieSum = 0;

    public void setup(Context context) throws IOException {


        String line;

        // Reads the rating counts of movies from the cache file
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path getFilePath = new Path("ratingCounts/part-r-00000");
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

        // A loop that runs on each movie records , that sets the number of movies per rating
        // and sums all the movies in the data
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\\s+");
            int rating = Integer.parseInt(split[0]);
            int numberOfItems = Integer.parseInt(split[1]);
            movieSum += numberOfItems;
            numMoviesPerRating[rating - 1] = numberOfItems;

        }


    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int fpCounts = 0;
        int fnCounts = 0;

        // Increments the FP and FN counts on each received value (0 for FP and 1 for FN)
        for (IntWritable val : values) {
            if (val.get() == 0) {
                fpCounts++;

            } else {
                fnCounts++;
            }

        }

        // Applying the formula stated before for the calculation of false positive probability rate
        double fpPercentage = (((double) fpCounts) / (movieSum - numMoviesPerRating[Integer.parseInt(key.toString()) - 1])) * 100;
        result.set("False Positive : " + fpCounts
                + " False Negatives : " + fnCounts +
                " False Positive Rate : " + String.format("%.2f", fpPercentage) + "%");

        // Output each pair of (K,V) that contains the bloom filter key rating and its statistics
        context.write(key, result);

    }


}