# Abstract
* The goal of the project is to design a MapReduce algorithm to implement a set of Bloom Filters in both Hadoop and Spark.
* A Bloom Filter is a space-efficient probabilistic data structure, which is used to establish if an element is a member of a set or not. It’s possible to obtain
false positives, but not false negatives.
* The important parameters to consider while projecting such a Filter are the size of the input set (n), the size of the filter in the memory, given in bits (m), the optimal number of Hash Functions to use (k), and the False Positives Rate (p).
* The input set here is given: the aim is to realize 10 Bloom Filters, each of which will store the films of the list with a fixated rounded evaluation. The Bloom Filter itself is a bit array initialized to m zeros. Adding an element means setting k of the m bits of the filter to 1, according to the Hash Functions.
* The attached report contains the most important information about the Projection of the Filter, the Algorithm and the associated Pseudo Code, the Hadoop and Spark implementations, and the Testing Phase.
