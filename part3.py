from mrjob.job import MRJob

class PartC(MRJob):


    def mapper(self, _, trans):
        try:
            fields = trans.split(',')
            if len(fields) == 9 :
                miners = fields[2]
                size = int(fields[4])
                yield(miners,size)
        except:
            pass

    def combiner(self, miners, size):
        yield(miners,sum(size))


    def reducer(self, miners, size):
        yield(miners,sum(size))


if __name__ == '__main__':
        PartC.run()

