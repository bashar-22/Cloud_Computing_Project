package it.unipi.hadoop;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This custom output format class is to enable us to read and  output the bloom-filters in a byte file format
 * Its usage is based on serialization and deserialization functions implemented in the bloomFilterWritable Class
 */

public class BloomFilterOutputFormat extends FileOutputFormat<NullWritable, BloomFilterWritable<String>> {

    protected static class BFRecordWriter extends RecordWriter<NullWritable, BloomFilterWritable<String>> {

        private final DataOutputStream out;

        public BFRecordWriter(DataOutputStream out) {
            this.out = out;
        }


        @Override
        public void write(NullWritable nullWritable, BloomFilterWritable<String> bf) throws IOException {
            bf.write(this.out);
        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException {
            out.close();

        }
    }

    public RecordWriter<NullWritable, BloomFilterWritable<String>> getRecordWriter(TaskAttemptContext job) throws IOException {


        Path file = getDefaultWorkFile(job, "");
        FileSystem fs = file.getFileSystem(job.getConfiguration());
        FSDataOutputStream fileOut = fs.create(file, false);
        return new BFRecordWriter(fileOut);

    }

}