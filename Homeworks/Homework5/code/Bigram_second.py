
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")
class Bigram_second(MRJob):  
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  combiner = self.combiner,
                  reducer=self.reducer),
            MRStep(reducer=self.reducer_top)
        ]

    def mapper(self, _, line):
        preword=""
        for word in WORD_RE.findall(line):
            if  preword !="" and word !="":
                value = (preword.lower(), word.lower())
                yield value, 1
            preword=word
            
    def combiner(self, bigram, counts):
        yield sorted(bigram), sum(counts)
        
    def reducer(self, bigram,count):
        yield None,(bigram, sum(count))
        
    def reducer_top(self, _ , bigram_count):
        for i in sorted(bigram_count, key=lambda x:x[1], reverse=True)[:10]:
                   yield i
            
if __name__ == '__main__':
     Bigram_second.run()