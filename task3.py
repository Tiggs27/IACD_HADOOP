from mrjob.job import MRJob
from mrjob.step import MRStep
import re 

class Book_dataset(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_words,reducer=self.reducer_count_words)
        ]
    
    def mapper_count_words(self, _, line):
        for word in re.split(" |\s|/.\.\s|[^a-zA-Z0-9]+/g",line):
            word = word.lower()
            yield word,1

    def reducer_count_words(self, key, values):
        values = list(values)
        new_value = sum(values)  
        yield key, new_value

     
if __name__ == '__main__':
    Book_dataset.run()

