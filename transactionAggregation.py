#this is part b job 1

from mrjob.job import MRJob

class PartBJob1(MRJob):


    def mapper(self, _, trans):
        try:
            fields = trans.split(',')
            if len(fields) == 7 :#check if the transactions are clean
                address=fields[2]#assigning transaction values
                value=int(fields[3])
                yield(address,value)# giving the address and its value
        except:
            pass

    def combiner(self, address, value):
        yield(address,sum(value))#Adding up the values for each address


    def reducer(self, address, value):
        yield(address,sum(value))#Adding up the values for each address

if __name__ == '__main__':
        PartBJob1.run()

