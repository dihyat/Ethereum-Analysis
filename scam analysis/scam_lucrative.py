
import pyspark
import json

sc = pyspark.SparkContext()
trans = sc.textFile('/data/ethereum/transactions')

def get_trans(line):
	try:
		data = line.split(",")
		if len(data) == 7:
			address = data[2]
			value = data[3]
			return(address,value)
	except:
		pass

def check_null(value):
	if value is None:
		return False
	return True

all_trans = trans.map(get_trans)
trans_mapped =  all_trans.filter(check_null)
trans_reduce = trans_mapped.reduceByKey(lambda x, y: x+y)


def all_scam(line):
	try:
		data = line.split(",")
		address = data[0]
		category = data[1]
		return(address, category)
	except:
		pass

scam = sc.textFile("input/scams.csv") 

all_scams = scam.map(all_scam)
scams_mapped  = all_scams.filter(check_null)
scams_join = scams_mapped.join(trans_reduce)
lucrative_scams = scams_join.takeOrdered(6, key=lambda x: -x[1][1]) 

for scam in lucrative_scams:
    print("{} /t {}".format(scam[0], scam[1]))
