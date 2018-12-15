
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
WORD_RE = re.compile(r"[\w']+")
class Trigram(MRJob):  
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                  combiner = self.combiner,
                  reducer=self.reducer),
            MRStep(reducer=self.reducer_top)
        ]

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for i, word in enumerate(words):
            if i < len(words) - 2:
                trigram = [words[i].lower(), words[i + 1].lower(), words[i + 2].lower()]
                yield trigram, 1
            
    def combiner(self, trigram, counts):
        yield trigram, sum(counts)
        
    def reducer(self, trigram,count):
        yield None,(trigram, sum(count))
        
    def reducer_top(self, _ , trigram_count):
        for i in sorted(trigram_count, key=lambda x:x[1], reverse=True)[:10]:
                   yield i
            
if __name__ == '__main__':
    Trigram.run()