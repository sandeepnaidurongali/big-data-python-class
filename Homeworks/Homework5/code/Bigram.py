
from mrjob.job import MRJob    
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")
class Bigram(MRJob):  
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  combiner = self.combiner,
                  reducer=self.reducer),
            MRStep(reducer=self.reducer_top)
        ]

    def mapper(self, _, line): # mapper function takes line as input and and other input as null value 
        preword=""
        for word in WORD_RE.findall(line):
            if  preword !="" and word !="":           
                yield (preword.lower(),word.lower()), 1
            preword=word
            
    def combiner(self, bigram, counts): # Each time calls the combiner by giving 2 words as bigram and value 1 to the counts 
        yield bigram, sum(counts)
        
    def reducer(self, bigram,count):  # Each time it have 2 words as bigram and produce none value and combination of bigram and count
        yield None,(bigram, sum(count))
        
    def reducer_top(self, _ , bigram_count): # It produces the top 10 highest frequency bigrams
        for i in sorted(bigram_count, key=lambda x:x[1], reverse=True)[:10]:
                   yield i
            
if __name__ == '__main__':
    Bigram.run()