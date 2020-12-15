from mrjob.job import MRJob

class topten(MRJob):

    def mapper(self, _, line):
        try:
            fields=line.split('\t')
            if len(fields)==2:
                address=fields[0][1:-1]
                value=int(fields[1])
                yield(None,(address,value))# giving the address and its value
        except:
            pass

    def combiner(self,_,values):
        sorted_values=sorted(values,reverse=True, key= lambda tup:tup[1])
        i=0
        for value in sorted_values:
            yield("top",value)
            i+=1
            if i>=10:
                break


    def reducer(self,_,values):
        sorted_values=sorted(values,reverse=True, key= lambda tup:tup[1])
        i=0
        for value in sorted_values:
            yield(i,("{} {}".format(value[0],value[1])))
            i+=1
            if i>=10:
                break


if __name__ =='__main__':
    topten.run()
