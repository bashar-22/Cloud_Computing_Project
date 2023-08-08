import math
from bitarray import bitarray
import mmh3

### Bloom filter class
class BloomFilter(object):

    def __init__(self, items_count, fp_prob=0.2):

        
        # False possible probability 
        self.fp_prob = fp_prob

        # Size of bit array to use
        self.size = self.round_half_up((-(items_count * math.log(fp_prob))/(math.log(2)**2)),0)


        # number of hash functions to use
        self.hash_count = math.ceil( (self.size/items_count) * math.log(2))

        # Bit array of given size
        self.bit_array = bitarray(self.size)

        # initialize all bits as 0
        self.bit_array.setall(0)

    def add(self, item):
        '''
        Add an item in the filter
        '''
        hashIndexes = []
        for i in range(self.hash_count):

            # create hash for given item.
            # i iterator work as seed to mmh3.hash() function
            # With different seed, hashes created are different
            
            hashIndex = abs(mmh3.hash(item, i) % self.size)
            hashIndexes.append(hashIndex)

            # set the bit to 1 in bit_array
            self.bit_array[hashIndex] = True
            
    def setIndex(self, index):
        '''
        Set an index in the filter to 1
        '''
        self.bit_array[index] = True
            
    def getHashIndexes(self, item):
        '''
        an array of hash indexes of an item is returned
        '''
        hashIndexes = []
        for i in range(self.hash_count):
            hashIndex = abs(mmh3.hash(item, i) % self.size)
            hashIndexes.append(hashIndex)

        return hashIndexes
            
    def union(self, bloomFilter):
        '''
        Performers an OR bitwise operation between two bloomfilters
        '''
        self.bit_array |= bloomFilter.bit_array

    def check(self, item):
        '''
        Check for existence of an item in filter
        '''
        for i in range(self.hash_count):
            hashIndex = abs(mmh3.hash(item, i) % self.size)
            if self.bit_array[hashIndex] == False:

                # if any of bit is False then,its not present
                # in filter
                # else there is probability that it exist
                return False
        return True
    
    # A fuction to mimick java's half up rounding  
    @classmethod
    def round_half_up(self, n, decimals=0):
        multiplier = 10 ** decimals
        return int(math.floor(n*multiplier + 0.5) / multiplier)
    