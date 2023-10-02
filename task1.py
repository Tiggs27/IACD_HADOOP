from mrjob.job import MRJob
from mrjob.step import MRStep
import sys


class Friends_Average(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_friends,reducer=self.reducer_avg_friends)
        ]
    
    def mapper_get_friends(self, _, line):
        (id, name, age, friends) = line.split(",")
        yield age,int(friends)

    def reducer_avg_friends(self, key, values):
        values = list(values)
        new_value = sum(values)/len(values)
        yield key, new_value

     
if __name__ == '__main__':
    Friends_Average.run()
 