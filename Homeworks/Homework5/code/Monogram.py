
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")
class Monogram(MRJob):  
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  combiner = self.combiner,
                  reducer=self.reducer),
            MRStep(reducer=self.reducer_top)
        ]

    def mapper(self, _, line):
          for word in WORD_RE.findall(line):
            yield word, 1
                        
    def combiner(self, monogram, counts):
        yield monogram, sum(counts)
        
    def reducer(self, monogram,count):
        yield None,(monogram, sum(count))
        
    def reducer_top(self, _ , monogram_count):
        for i in sorted(monogram_count, key=lambda x:x[1], reverse=True)[:10]:
                   yield i
            
if __name__ == '__main__':
     Monogram.run()