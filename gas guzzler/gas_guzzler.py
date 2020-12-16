import pyspark
import time
import traceback


sc = pyspark.SparkContext()



def get_trans(line):
    try:
        data = line.split(",")
        if len(data) == 7:
            timestamp = time.gmtime(int(data[6]))
            date = time.strftime("%m-%Y", timestamp)
            price = int(data[5])
            return (date, (price, 1))
    except:
        pass
        

data = sc.textFile("/data/ethereum/transactions")

data_mapped = data.map(get_trans)

def check_null(value):
    if value is None:
        return False
    elif len(value) != 2:
        return False
    elif value[0] is None:
        return False
    elif len(value[1]) is None:
        return False
    return True

gas_data = data_mapped.filter(check_null)
gas_reduced = gas_data.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

gas_average = gas_reduced.mapValues(lambda x: x[0] / x[1])
gas_sort = gas_average.sortByKey()

sorted.saveAsTextFile("gas_guzzler.txt")



