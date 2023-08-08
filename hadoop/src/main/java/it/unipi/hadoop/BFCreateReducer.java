package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import static java.lang.Double.*;

/**
 * This Reducer has the goal of appending each hash index received in order to
 * set the bits in 10 bloom-filters and afterwards output each bloom-filter in
 * a separate byte file using MultipleOutputs class.
 *
 * Setup() function is used to set up each of 10 empty bloom-filters using the number of elements
 * and the probability rate.
 *
 * Reduce() function sets the bits in each bloom-filter and outputs it
 */

public class BFCreateReducer
        extends Reducer<Text, IntWritable, NullWritable, BloomFilterWritable<String>> {
    MultipleOutputs mos;
    HashMap<String, BloomFilterWritable<String>> bloomFilters;

    public void setup(Context context) throws IOException {

        mos = new MultipleOutputs(context);
        bloomFilters = new HashMap<>();
        String line;

        Configuration conf = context.getConfiguration();

        // Read the false positive rate from the global configuration variable
        double falsePositiveRate = parseDouble(conf.get("fpRate"));

        // Reads the movies count per rating from the cache file
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path getFilePath = new Path(cacheFiles[0].toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

        // Initializes each bloom-filter with the size and number of hash functions
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\\s+");
            String rating = split[0];
            int numberOfItems = Integer.parseInt(split[1]);
            bloomFilters.put(rating, new BloomFilterWritable<>(numberOfItems, falsePositiveRate));
        }

    }

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
        // Retrieves the bloom-filter to assign bits to from the hash map
        BloomFilterWritable<String> bf = bloomFilters.get(key.toString());

        // Looping for each index and setting its bit
        for (IntWritable val : values) {
            bf.setIndex(val.get());
        }
        mos.write(key.toString(), NullWritable.get(), bf);

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}