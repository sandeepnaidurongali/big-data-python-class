from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")


class MRTrigramFreqCount(MRJob):

    def mapper(self, _, line):
        words = WORD_RE.findall(line)
        for i, word in enumerate(words):
            if i < len(words) - 2:
                trigram = [words[i].lower(), words[i + 1].lower(), words[i + 2].lower()]
                yield trigram, 1 
                
    def combiner(self, key, counts):
        yield sorted(key), sum(counts)

    def reducer(self, key, counts):
        yield sorted(key), sum(counts)
        

if __name__ == '__main__':
    MRTrigramFreqCount.run()