import sys
import math
from bfClass import BloomFilter
from pyspark import SparkContext

falsePositiveRate=0.2



# A fuction to mimick java's half up rounding
def round_half_up(n, decimals=0):
    multiplier = 10 ** decimals
    return int(math.floor(n*multiplier + 0.5) / multiplier)


def map1(line):
    result=line.split()
    roundedRatings =round_half_up((float(result[1])))
    return (roundedRatings,1)

def map2(line,bloomFilters): 
    result=line.split() 
    movieId=result[0] 
    rating=round_half_up(float(result[1])) 
    bloomFilter=bloomFilters.get(rating) 
    hashIndexes = bloomFilter.getHashIndexes(movieId)
    return(rating,hashIndexes)

def reducer1(a, b):
    return a+b

def reducer2(a, b):
    for x in b:
        a.append(x)
    return a

    
# Usage : <input1> : Dataset file || <input2> : FP Probability Rate ||  <output> : BloomFilter 

if __name__ == "__main__":
    

    # Setup Spark Context with the bloom Filter class

    sc = SparkContext("yarn", "BF Creation Phase",pyFiles=['bfClass.py'])
    
    # Create Input file RDD and Read FP Rate
    lines = sc.textFile(sys.argv[1])
    
    # In order to mimick the NLineInputFormat on hadoop which
    # takes N lines in a split , we are going to use repartion
    # function to split the Input in equal 4 partitions
    lines_splitted = lines.repartition(4)

    # Reads the false positive rate from the arguments
    falsePositiveRate = float(sys.argv[2])

    # Persist the RDD in memory after the first calculation to save recomputations cost in second use
    lines_splitted.cache()
    
    # 1st Map-Reduce Job 
    # Computes How Many Movies Per Rating, output is list of (K,V) where K = rating & V = count
    ratingCounts = lines_splitted.map(map1).reduceByKey(reducer1)
    ratingCounts_InMemory = ratingCounts.collect()

    # Save the rating counts pickled in HDFS for later use in test phase
    ratingCounts.saveAsPickleFile("spark_ratingCounts")
    
    # Initialize bloomfilters and assign them in a dictionnary
    
    bloomFilters = {}  
    for (rating, count) in ratingCounts_InMemory: 
        bloomFilters[rating] = BloomFilter(count,falsePositiveRate) 
        
    # 2nd Map-Reduce job 
    # Mapper outputs a list of (K,V) , K = (BF key) & V = (list of an movieID hashed indexes)
    mapped_rdd =lines_splitted.map(lambda line : map2(line,bloomFilters))
    
    # Reducer return the aggregation of hashed indexes list for each BF
    result_rdd = mapped_rdd.reduceByKey(reducer2).sortByKey().collect()
    
    # Set each bloomfilter with the collected list of hashed indexes
    for i in range(10):
        for index in result_rdd[i][1]:
            bloomFilters[i+1].setIndex(index)
            
    # Save Bloomfilters into HDFS as a pickled file
    bfRDD = sc.parallelize(list(bloomFilters.items()))
    bfRDD.repartition(1).saveAsPickleFile(sys.argv[3])