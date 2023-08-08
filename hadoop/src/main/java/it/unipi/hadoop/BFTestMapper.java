package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * This Mapper has the goal of counting the number of false positive,
 * false negative  for each Bloom-filter in order to assess
 * the theoretical value chosen at creation.
 *
 * A false positive : Movie ID contained in a bloom-filter but in incorrect one.
 * A false negative : Movie not contained in a bloom-filter which is supposed to be.
 *
 * Setup() function reads each bloom-filter byte file to be used in mapper processes
 *
 * Map() function performs the test on each movie record on each bloom-filter and output results
 * as (K,V) pair where K = Rating , V = 0 or 1 ; 0 stands for FP and 1 Stands for FN
 */

public class BFTestMapper
        extends Mapper<Object, Text, Text, IntWritable> {

    private final Text outKey = new Text();
    private final IntWritable outValue = new IntWritable(0);
    List<BloomFilterWritable<String>> bloomFilters;


    public void setup(Context context) throws IOException {
        // Initializes an array that holds the bloom-filters
        bloomFilters = new ArrayList<>();

        // Deserialize each byte file and insert the bloom filter in the array
        // each file is read from the cache on the nodes

        URI[] cacheFiles = context.getCacheFiles();
        for (URI cacheFile : cacheFiles) {
            Path getFilePath = new Path(cacheFile.toString());

            final FSDataInputStream in = getFilePath.getFileSystem(
                    new Configuration()).open(getFilePath);
            BloomFilterWritable<String> bf = new BloomFilterWritable<>();
            bf.readFields(in);
            bloomFilters.add(bf);
            in.close();
        }

    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


        String str = value.toString();
        String[] split = str.split("\\s+");

        // Extract the rating and movie of each record on data input
        int movieRating = (int) Math.round(Double.parseDouble(split[1]));
        String movieTitle = split[0];

        // Run the movie ID presence check on each bloom-filter
        for (int i = 1; i <= 10; i++) {

            outKey.set(String.valueOf(i));

            // A condition that checks if the movie is contained in a BF
            boolean bfContained = bloomFilters.get(i - 1).contains(movieTitle);

            // A condition that checks if the movie is in the correct BF
            boolean correctBf = (movieRating == (i));

            if (bfContained && !correctBf) {   // False positive check
                outValue.set(0);
                context.write(outKey, outValue);

            } else if (!bfContained && correctBf) {   // False negative check
                outValue.set(1);
                context.write(outKey, outValue);

            }
        }

    }
}