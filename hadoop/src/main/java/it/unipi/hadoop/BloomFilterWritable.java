package it.unipi.hadoop;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.MurmurHash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;

public class BloomFilterWritable<E> implements Writable {

    // Bit is a java array of bits that will use to hold the bloom-filters indexes
    public BitSet bf;

    // Determines the bloom-filter size
    public int bitArraySize;

    // The number of hash functions used
    public int numHashFunc;

    // Empty constructor
    public BloomFilterWritable() {

    }

    // A constructor that clones another bloomfilter
    public BloomFilterWritable(BloomFilterWritable<E> obj) {
        this.bitArraySize = obj.bitArraySize;
        this.numHashFunc = obj.numHashFunc;
        this.bf = new BitSet();
        this.bf = (BitSet) obj.bf.clone();

    }

    // Initializes an empty bloom-filter , supplying the number of elements to hold and
    // the false positive rate chosen, it uses the optimal formula of number of hash
    // function calculation and the optimal bit array size
    public BloomFilterWritable(int numOfElements, double falsePositiveRate) {
        bitArraySize = (int) Math.round(-(numOfElements * Math.log(falsePositiveRate)) / (Math.pow(Math.log(2), 2.0)));
        numHashFunc = (int) Math.ceil(((double) bitArraySize / numOfElements) * Math.log(2));
        bf = new BitSet(numOfElements);

    }

    // Inserts an element into the bloom-filter and sets their indexes in the BF
    public void add(E obj) {
        int[] indexes = getHashIndexes(obj);

        for (int index : indexes) {
            bf.set(index);
        }
    }

    // Sets an index manually in the bloom-filter
    public void setIndex(int index) {
        bf.set(index);

    }

    // Checks if an element is contained in the bloom filter depending on
    // the number of hash functions. If all indexes are contained the item is contained
    // if one of them is not then return false.
    public boolean contains(E obj) {
        int[] indexes = getHashIndexes(obj);

        for (int index : indexes) {
            if (!bf.get(index)) {
                return false;
            }
        }

        return true;
    }

    // Performs a union between the BF called and its arguments.
    // It consists of logical OR operation bit by bit
    public void union(BloomFilterWritable<E> other) {

        bf.or(other.bf);
    }

    protected int[] getHashIndexes(E obj) {
        int[] indexes = new int[numHashFunc];

        MurmurHash md = new MurmurHash();

        /*
         * creates hash for given item.
         * i iterator work as seed to murmur hash  function
         * With different seed, hashes created are different
        */

        for (int seed = 0, i = 0; i < numHashFunc; i++, seed++) {
            int hashValue = md.hash(obj.toString().getBytes(), seed);
            indexes[i] = Math.abs(hashValue % (bitArraySize - 1));

        }

        return indexes;
    }
    // Serialize the bloom-filter and output its size and number of hash function
    // and the bit array itself in a byte array
    public void write(DataOutput out) throws IOException {
        int byteArraySize = (int) Math.ceil(bitArraySize / 8.0);
        byte[] byteArray = new byte[byteArraySize];

        for (int i = 0; i < byteArraySize; i++) {
            byte nextElement = 0;
            for (int j = 0; j < 8; j++) {
                if (bf.get(8 * i + j)) {
                    nextElement |= 1 << j;
                }
            }

            byteArray[i] = nextElement;
        }
        out.writeInt(bitArraySize);
        out.writeInt(numHashFunc);
        out.write(byteArray);
    }

    // Deserialize the bloom-filter byte array and read its data into the object
    public void readFields(DataInput in) throws IOException {
        bitArraySize = in.readInt();
        numHashFunc = in.readInt();
        bf = new BitSet(bitArraySize);

        int byteArraySize = (int) Math.ceil(bitArraySize / 8.0);
        byte[] byteArray = new byte[byteArraySize];
        in.readFully(byteArray);

        for (int i = 0; i < byteArraySize; i++) {
            byte nextByte = byteArray[i];
            for (int j = 0; j < 8; j++) {
                if (((int) nextByte & (1 << j)) != 0) {
                    bf.set(8 * i + j);
                }
            }
        }
    }

    // Output the bloom filter as a string in the format of :  { 1,2,45 }
    // where the items are the bits set to true in bit array
    public String ToString() {
        return bf.toString();

    }


}
