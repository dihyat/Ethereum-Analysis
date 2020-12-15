from mrjob.job import MRJob
import time

class Trans_num (MRJob):
	def mapper(self,_, line):
		try:
			fields=line.split(',')
			if len(fields) ==7:
				time_epoch = int(fields[6])
				day = time.strftime("%m-%Y", time.gmtime(time_epoch))
				yield(day,1)
		except:
			pass

	def combiner(self, day, counts):
		yield (day,sum(counts))

	def reducer(slf,day,counts):
		yield(day,sum(counts))

if __name__== '__main__':
	Trans_num.run() 
