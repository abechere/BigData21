#this will save this cell as file

from mrjob.job import MRJob
import csv

class MRWordCount(MRJob):

    def mapper(self, _, line): 
        reader = line.split(',') 
        salary = reader[-2] 
        grosspay=reader[-1] 
        
        try: 
            float(salary) 
            float(grosspay) 
        except: 
            salary = 0 
            grosspay = 0 
        yield("topentries", (float(salary), float(grosspay))) 
        
    def reducer(self, key, values): 
        salary, grosspay = zip(*values) 
        salary, grosspay = list(salary), list(grosspay) 
        salary.sort(reverse=True) 
        grosspay.sort(reverse=True) 
        yield(salary[:10], grosspay[:10]) 


if __name__ == '__main__':
    MRWordCount.run()
