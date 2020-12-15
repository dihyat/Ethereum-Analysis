#This is part b job 2

from mrjob.job import MRJob

class Join(MRJob):

    def mapper(self, _, line):
        try:
            if(len(line.split(',')) == 5):
                fields=line.split(',')
                join_key=fields[0]
                join_value=1
                yield(join_key,(join_value,1))

            elif(len(line.split('\t'))==2):
                fields=line.split('\t')
                join_key=fields[0]
                join_key=join_key[1:-1]#removes the speech mark at the end
                join_value=int(fields[1])
                yield(join_key, (join_value,2))# giving the address and its value

        except:
            pass

    def reducer(self,address,values):
        contract=0
        transaction=0
        for value in values:
            if value[1]==1:
                contract=value[0]
            elif value[1]==2:
                transaction=value[0]
        if((contract==1) and transaction!=0):
            yield(address,transaction)

if __name__ =='__main__':
    Join.run()

