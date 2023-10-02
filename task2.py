from mrjob.job import MRJob
from mrjob.step import MRStep


class Temperature_per_Capital(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_temperature,reducer=self.reducer_min_temperature)
        ]
    
    def mapper_get_temperature(self, _, line):
        (weather_station, date, observation_type, temperature,_,_,_,_) = line.split(",")
        yield weather_station,int(temperature)

    def reducer_min_temperature(self, key, values):
        values = list(values)
        new_value =min(values)/10
            
        yield key, new_value
     
if __name__ == '__main__':
    Temperature_per_Capital.run()

