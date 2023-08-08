import sys
import math
from bfClass import BloomFilter
from pyspark import SparkContext




# A fuction to mimick java's half up rounding
def round_half_up(n, decimals=0):
    multiplier = 10 ** decimals
    return math.floor(n*multiplier + 0.5) / multiplier


def mapF(line,bloomFilters):
    result=line.split()
    movieRating = round_half_up((float(result[1])))
    movieId = result[0]
    for i in range(1,11):
        bfContained = bloomFilters[i].check(movieId);
        correctBf= (int(movieRating) == (i))
        
        if bfContained and not correctBf:  #False Postive
            yield(i,0)
        elif not bfContained and correctBf: # False Negative
            yield(i,1)
    

# Usage : <input1> : Training set file || <input2> : BloomFilters Input ||  <output> : Test Phase Results 

if __name__ == "__main__":
    
    
    # Setup Spark Context with the bloom Filter class

    sc = SparkContext("yarn", "BF Test Phase",pyFiles=['bfClass.py'])
    
    # Create RDDs of input files and pickled rating counts
    lines = sc.textFile(sys.argv[1])
    bloomFilters_RDD = sc.pickleFile(sys.argv[2])
    ratings_RDD = sc.pickleFile("spark_ratingCounts").collect()
    
    # In order to mimick the NLineInputFormat on hadoop which
    # takes N lines in a split , we are going to use repartion
    # function to split the Input in equal 4 partitions
    lines_splitted = lines.repartition(4)
    
    # Insert ratings into a dictionnary , and sum the total num of movies
    ratings = {}
    movieSum=0
    for (rating,count) in ratings_RDD:
        movieSum+=count
        ratings[rating]=count
    
    # Assign bloomFilters from RDD to memory
    bloomFilters_InMemory=bloomFilters_RDD.collect()
    
    
    # Assign each BF into a dictionnary
    bloomFilters={}
    for (rating, bloomFilter) in bloomFilters_InMemory: 
        bloomFilters[rating] = bloomFilter

    # Map each movie record to output False Postive and False Negative Results In Each BF
    mappedValues = lines_splitted.flatMap(lambda line: mapF(line, bloomFilters))
    
    # Persist the RDD in memory after the first calculation to save recomputations cost in second use
    mappedValues.cache()
    
    # Count for each BF the Number of FP and FN
    numFP = mappedValues.filter(lambda x: x[1]==0).countByKey()
    numFN = mappedValues.filter(lambda x: x[1]==1).countByKey()
    

    
    # Insert each BF result in a dictionnary in string format
    rates={}
    for i in range(1,11):
        
        fpRate= str(round((numFP[i] / (movieSum - ratings[i])) * 100, 2)) # False Postive Percentage Formula
        fpCounts = str(numFP[i] if i in numFP else 0)
        fnCounts = str(numFN[i] if i in numFN else 0)
        rates[i]="False Positive : " + fpCounts + " False Negatives : " + fnCounts + " False Positive Rate : " + fpRate + "%"
    
    # Output Test Results To HDFS
    results = sc.parallelize(rates.items())
    results.repartition(1).saveAsTextFile(sys.argv[3])

        
    
    
