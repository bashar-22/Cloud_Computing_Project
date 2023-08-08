package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * This Mapper has the goal of obtaining the hashed indexes of each Movie ID
 * where the hashed indexes ( a list of bits to set in the bit array depending
 * on the number of hash functions). This is done for each bloom-filter where it
 * depends on its size and the number of hash functions.
 * <p>
 * Setup() function initializes 10 empty BF depending on the number of elements and probability
 * rate, it is why we read the rating counts from cache.
 * <p>
 * Map() functions runs on each movie record and outputs the hashed indexes
 * for each BF as K,V pair where K = Bloom Filter key , V = Hashed Index
 */
public class BFCreateMapper
        extends Mapper<Object, Text, Text, IntWritable> {


    // Hashmap that holds the bloom-filters
    HashMap<Integer, BloomFilterWritable<String>> bloomFilters;
    Text keyRating = new Text();
    IntWritable indexValue = new IntWritable(0);

    public void setup(Context context) throws IOException {
        bloomFilters = new HashMap<>();
        String line;
        Configuration conf = context.getConfiguration();

        // Read the false positive rate from the global configuration variable
        double falsePositiveRate =Double.parseDouble(conf.get("fpRate"));

        // Read rating counts of movies from cache file
        URI[] cacheFiles = context.getCacheFiles();
        FileSystem fs = FileSystem.get(conf);
        Path getFilePath = new Path(cacheFiles[0].toString());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

        // Assign each of the bloom-filters the number of items and probability rating
        while ((line = reader.readLine()) != null) {
            String[] split = line.split("\\s+");
            int rating = Integer.parseInt(split[0]);
            int numberOfItems = Integer.parseInt(split[1]);
            bloomFilters.put(rating, new BloomFilterWritable<>(numberOfItems, falsePositiveRate));
        }


    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Split movie records to extract the rating and ID
        String str = value.toString();
        String[] split = str.split("\\s+");
        String movieId = split[0];
        int movieRating = (int) Math.round(Double.parseDouble(split[1]));

        // Extract the hashed indexes for each bloom filter for each movie ID
        BloomFilterWritable<String> bf = bloomFilters.get(movieRating);
        int[] hashIndexes = bf.getHashIndexes(movieId);

        // Output each bloom-filter in a separate file
        // as a (K,V) pair where K= rating, V= bloom-filter
        for (int index : hashIndexes) {
            keyRating.set(Integer.toString(movieRating));
            indexValue.set(index);
            context.write(keyRating, indexValue);
        }

    }

}
