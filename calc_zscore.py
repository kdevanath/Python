import h5py
import numpy as np
import io
from collections import defaultdict
import csv
from operator import itemgetter, attrgetter
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-s", "--start", dest="start_date",default="2005-01-01",
                  type="string",help="start date <YYYY-mm-dd>", action="store")
parser.add_option("-o", "--obs",type="int",
                  action="store", dest="obs", default=200,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()
print("Calculating")
def MA(length,start_sd,basis,) :
	m=[]
	d=[]
	sd=[]
	z=[]
#	m.append(basis[0])
	n = len(basis)
	obs = start_sd-1
	for i,spot in enumerate(basis):
		if(i==0):
			m.append(basis[0])
			d.append(0.0)
			z.append(0.0)
			sd.append(0.0)
			continue
		else:
			m.append(((1/length) * spot + ((1-(1/length)) * m[len(m)-1])))
		d.append(basis[i]-m[i])
		if(i- obs>= 0):
			start = i-obs
			#a = np.array([d[start:i]])
			#print(np.std(a),i,start)#sd.append(np.std(d[start:i]))
			sd.append(np.std(d[start:i]))
			#print(d[i]/sd[start])
			z.append(d[i]/sd[len(sd)-1])
		else:
			z.append(0.0)
			sd.append(0.0)
	#print(np.max(d),np.min(d),np.std(d))
	return m,d,sd,z
#
#ranking=defaultdict(list)
f1=h5py.File("P:\\FSB\\CEF\\hdf5\\cef_download.hdf5","r")
sfile ="P:\\FSB\\CEF\\data\\CEF_Simulate.csv"
symbols=np.recfromcsv(sfile)
for s in symbols:
	category=s[0]
	category_txt = category.decode(encoding='windows-1252')
	name=s[4]+s[5]
	name_txt = name.decode(encoding='windows-1252')
	dset_name="/yahoo/HP/"
	dset_name+=(category_txt+"/")
	dset_name+=name_txt
#	print(dset_name)
	#dset2=f1['/yahoo/HP/CEF_EQUITY/USAXUSAX']
	try:
		dset2=f1[dset_name]
	except:
		print("Cannot find %s" %(dset_name))
		continue
#	print(dset2[1])
	st_date=options.start_date
	num_obs=options.obs
	print(st_date,num_obs)
#	no_zero_dset = np.array([a_row for a_row in dset2 if ((st_date < bytes.decode(a_row['date'])) & (a_row['price']>0.0))],dtype=dset2.dtype)
	no_zero_dset = dset2[(dset2['date'] > st_date.encode()) & (dset2['price']>0.0)]
#	print(no_zero_dset.dtype)
	# try:
		# no_zero_dset = newdset[newdset[:,'price'] > 0.]
	# except ValueError as v:
		# print(v,newdset)
	print(no_zero_dset[0][1])
	basis=((no_zero_dset['close']/no_zero_dset['price']) -1)*100
#	print(basis)
	ma10,d10,sd10,z10 = MA(10,num_obs,basis)
	#print(m10[0],m10[1],m10[2],m10[len(m10)-1],d10[0],d10[1],d10[2],d10[len(d10)-1],sd10[0],sd10[1],sd10[2],sd10[len(sd10)-1])
	ma20,d20,sd20,z20 = MA(20,num_obs,basis)
	# print(ma20[0],ma20[1],ma20[2],ma20[len(ma20)-1])
	ma40,d40,sd40,z40 = MA(40,num_obs,basis)
	# print(ma40[0],ma40[1],ma40[2],ma40[len(ma40)-1])
	ma80,d80,sd80,z80 = MA(80,num_obs,basis)
	# print(ma80[0],ma80[1],ma80[2],ma80[len(ma80)-1])
	ma150,d150,sd150,z150 = MA(150,num_obs,basis)
	# print(ma150[0],ma150[1],ma150[2],ma150[len(ma150)-1])
	ma200,d200,sd200,z200 = MA(200,num_obs,basis)
	# print(ma200[0],ma200[1],ma200[2],ma200[len(ma200)-1])
#	f1.close()
	a =  np.rec.fromarrays([z10,z20,z40,z80,z150,z200],names='z10 z20 z40 z80 z150 z200')
	# if(len(a) > 199):
		# print(sum(a[199]))
	f = open(("P:\\FSB\\CEF\zscore\\zs_"+name_txt+".csv"),'w')
	csv_wr = csv.writer(f,delimiter=',',quoting=csv.QUOTE_MINIMAL)
	csv_wr.writerow(('Name','Date','basis_spot','MA10','MA20','MA40','MA80','MA150','MA200',
	'DEV10','DEV20','DEV40','DEV80','DEV150','DEV200',
	'STDDEV_10','STDDEV_20','STDDEV_40','STDDEV_80','STDDEV_150','STDDEV_200',
	'Z_10','Z_20','Z_40','Z_80','Z_150','Z_200','Z_AVG','PREV','CURRENT'))
	print("Starts from %s , %s"  %(name_txt,(no_zero_dset[0][1]).decode(encoding='windows-1252')))
	prev_close=0.0
	for i,spot in enumerate(basis):	
		zscore=sum(a[i])/float(len(a[i]))
		date_txt=(no_zero_dset[i][1]).decode(encoding='windows-1252')
		csv_wr.writerow((name_txt,date_txt,spot,ma10[i],ma20[i],ma40[i],ma80[i],ma150[i],ma200[i],
		d10[i],d20[i],d40[i],d80[i],d150[i],d200[i],
		sd10[i],sd20[i],sd40[i],sd80[i],sd150[i],sd200[i],
		z10[i],z20[i],z40[i],z80[i],z150[i],z200[i],zscore,prev_close,no_zero_dset[i]['adj_close']))
		prev_close=no_zero_dset[i]['adj_close'] 
		# if(zscore != 0.0):
			# ranking[date_txt].append([name_txt,zscore])
	f.close()
f1.close()
# f2=open("ranking.txt",'w')
# for k,v in ranking.items():
	# print(k,sorted(v,key=itemgetter(1)),file=f2)
	
	
