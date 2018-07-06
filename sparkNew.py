from pyspark import SparkContext
import json
sc=SparkContext.getOrCreate()

#Baca data dari storage hdfs
rdd = sc.textFile("hdfs://192.168.43.154:8020/xxxx.json")


#Cek rentang waktu
#2 parameter, ex: cek 17 mei - 20 mei
#3 parameter, ex: status 'ancaman' pada 17 mei - 20 mei
def cekRentang(waktux,waktuy,status=None,mean=False):
        def _cekRentang(data):
                dataJson=json.loads(data)
                temp=[]
		count=0
		avg=0
                waktuxSplit=waktux.split("/")
                waktuySplit=waktuy.split("/")
                for i in dataJson:
			if status == "bahaya":
                                if dataJson[str(i)]["status"]==status:
					temp.append(dataJson[i])
					#count+=1
			else:
				temp.append(dataJson[i])

		if mean==True:
			avg=float(len(temp))/len(dataJson)
			return avg
		
				
		return temp
        return _cekRentang

#rddA=rdd.map(cekTgl("15 mei"))
#rddA=rdd.map(cekRentang("16 mei", "17 mei")) #output: {"1":{ "status":"ancaman", "waktu":"16 mei 03:00" }, "2":{ "status":"aman", "waktu":"17 mei 12:00"}}
rddA=rdd.map(cekRentang("15/05/2018","31/05/2018","bahaya",mean=False)) #output:{"1":{"status":"ancaman","waktu":"16 mei 03:00"}}
rddRata=rdd.map(cekRentang("15/05/2018","31/05/2018","bahaya",mean=True))

#simpan dalam file
rddA.saveAsTextFile("hdfs://192.168.43.154:8020/hasil_dummy")
rddRata.saveAsTextFile("hdfs://192.168.43.154:8020/hasil_dummy_rata")

