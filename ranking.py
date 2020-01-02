import sys
import h5py
import numpy as np
from collections import defaultdict
from operator import itemgetter
import csv
from optparse import OptionParser
parser = OptionParser()
parser.add_option("-s", "--start", dest="start_date",default="2005-01-01",
                  type="string",help="start date <YYYY-mm-dd>", action="store")
parser.add_option("-r", "--rank",type="int",
                  action="store", dest="rank", default=20,
                  help="don't print status messages to stdout")
parser.add_option("-p", "--hold",type="int",
                  action="store", dest="hold", default=30,
                  help="don't print status messages to stdout")

(options, args) = parser.parse_args()
sfile ="P:\\FSB\\cef\\data\\CEF_Simulate.csv"
sim_symbols=np.recfromcsv(sfile)
ranking=defaultdict(list)
drm=defaultdict(list)
hdg_drm={}
lp={}
sp={}
symbols=[]
holding=defaultdict()
f1=h5py.File("P:\\FSB\\CEF\\hdf5\\cef_download.hdf5","r")
list(f1)
dset_name="/yahoo/HP/CEF_HEDGE/SPY"
dset2=f1['/yahoo/HP/CEF_HEDGE/SPY']	
st_date=options.start_date
rank=options.rank
holding_period=options.hold
print(st_date,rank,holding_period)
hedge=dset2[dset2['date']>=st_date.encode()]
f1.close()
#print(hedge[:]['date']) #prints  the dates from the each row
#hedge = np.array([a_row for a_row in dset2 if(st_date < bytes.decode(a_row['date']))],dtype=dset2.dtype)
# new_dt='2011-09-08'
# b = hedge[hedge['date']==new_dt.encode()] # gives me an array,b['ad_close'] actually gives me an array of that column
# print(b.ndim)
# print(b[0]['adj_close'],b['adj_close'])
# print(hedge[hedge['date']==dt.encode()])
# # for a_row in dset2 if(st_date < bytes.decode(a_row['date'])):
	# # hdg_row[a_row['date']]
# hdg_row=hedge[hedge[:,'date']==bytes.encode('2011-09-08')]
prev_dr=0.0
for a_row in hedge:
	if(prev_dr != 0.0):
		hdg_drm[a_row['date'].decode(encoding='windows-1252')] = a_row['adj_close'] #((a_row['adj_close']/prev_dr)-1)*10000
	else:
		hdg_drm[a_row['date'].decode(encoding='windows-1252')] = 0.0
		print(a_row['date'],a_row['adj_close'])
	prev_dr=a_row['adj_close']
for s in sim_symbols:
	name=s[4]+s[5]
	name_txt=name.decode(encoding='windows-1252')
	fname_txt="P:\\FSB\\CEF\\zscore\\zs_"
	fname_txt += name_txt
	category=s[0].decode(encoding='windows-1252')
	try:
		zscore=np.recfromcsv(fname_txt + ".csv")
	except IOError as e:
		print("Cannot find %s" %(fname_txt))
		continue
	print(fname_txt,category)
	symbols.append(name_txt)
	holding[name_txt]=[-1,0.0,0.0]
	for row in zscore:
		date_txt=row[1].decode(encoding='windows-1252')
		if(row['z_avg'] != 0.0):	
			ranking[date_txt].append([name_txt,category,row['z_avg'],row['prev'],row['current']])			
			if(row['prev'] != 0.0):
				drm[(name_txt,date_txt)]=((row['current']/row['prev'])-1)*10000#.append((row['current']/row['prev'])-1)	
				lp[(name,date_txt)]=0.0
				sp[(name,date_txt)]=0.0
print("Ranking")
prev_close=0.0
start=0
for k,v in sorted(ranking.items(),key=lambda x: x[0]):
	rankfile="P:\\FSB\\CEF\\ranking\\"+k
	f2=open( (rankfile +".txt"),'w')
#	hdg_drm[k]=0.0
#	hdg_row=hedge[hedge['date']==k.encode()]
	# if(prev_close != 0.0):
		# hdg_drm[k]=((hdg_row[0]['adj_close']/prev_close)-1)*10000
	# prev_close=hdg_row[0]['adj_close']
	s=sorted(v,key=itemgetter(2))
	top20=s[:rank]
#	rich=s[-rank:]
	for name,c,zs,prev,current in s:
		if(any(row[0] == name for row in top20)):
			if(holding[name][0] == -1):
#				print(holding[name],name,k)
				holding[name]=[start,current,hdg_drm[k]]
		if(holding[name][0] != -1):
			holding[name][0]+=1
			lp[(name,k)] = ((current/holding[name][1])-1)*10000 
			if(c =='CEF_EQUITY'):
				sp[(name,k)] = ((hdg_drm[k]/holding[name][2])-1)*10000
		if(holding[name][0] > holding_period):
#			print(holding[name],name,current,k)
			holding[name]=[-1,0.0,0.0]		
		print(name,zs,prev,current,k, sep=",", file=f2)
	f2.close();
header="Date,SPY,LP_PNL,SP_PNL,PORT,"
for name in sorted(symbols):
	header+=name+","
sim_file = "P:\\FSB\\CEF\\sim\\"+st_date+"_"+str(rank)+"_"+str(holding_period)+".csv"
f = open(sim_file,'w')
print(header,file=f)
for dt in sorted(ranking.keys()):
	# csv_wr = csv.writer(f,delimiter=',',quoting=csv.QUOTE_MINIMAL)
	# csv_wr.writerow(line0)
	pnl_line=dt+","+str(hdg_drm[dt])+","
	line=","
	sp_pnl = 0.0
	lp_pnl=0.0
	for name in sorted(symbols):
		if( (name,dt) in lp.keys()):
			lp_pnl+=lp[(name,dt)]
			line+=str(lp[(name,dt)])+","
		else:
			line+="0.0"+","
		if( (name,dt) in sp.keys()):
			sp_pnl+=sp[(name,dt)]
	pnl_line+=str(lp_pnl/rank)+","+str(sp_pnl/rank)+","+str( (lp_pnl/rank) - (sp_pnl/rank))
	print(pnl_line+line,file=f)
f.close()
	