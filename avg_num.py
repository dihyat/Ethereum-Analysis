from mrjob.job import MRJob
import time
from statistics import mean

class Avg_num (MRJob):
	def mapper(self,_, line):
	
		try:
			fields=line.split(',')
			if len(fields) ==7:
				time_epoch = int(fields[6])
				day = time.strftime("%Y-%m", time.gmtime(time_epoch))
				yield(day,(int(fields[3]),1))
		except:
			pass

	def combiner(self, feature, values):
		count=0
		total = 0
		for value in values:
			count +=value[1]
			total +=value[0]
		yield(feature,(total,count))

	def reducer(self,feature,values):
		count =0
		total = 0
		for value in values:
			count+=value[1]
			total+=value[0]
		yield(feature,total/count)
		

	

if __name__== '__main__':
	Avg_num.run() 
