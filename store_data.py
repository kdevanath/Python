import h5py
import numpy as np
import io
import datetime as dt
print("Starting")
f = h5py.File("P:\\FSB\\CEF\\hdf5\\cef_download.hdf5",'w')
sfile ="P:\\FSB\\CEF\\data\\CEF_Symbols.csv"
symbols=np.recfromcsv(sfile)
for s in symbols:
	dfile="P:\FSB\CEF\\data\\"
	fname= str()
	fname = bytes.decode(s[4])
	if(bytes.decode(s[5]) != "N/A"):
		fname+="_"
		fname+=bytes.decode(s[5])
	fname+=".csv"
	dfile+=fname
	print(dfile)
	kwargs = dict(delimiter=",",
				  names=("Symbol","Date","O","H","L","C","V","AC","D","NAVS","NAV"),
				  dtype=('S5','S5',float,float,float,float,int,float,float,'S5',float),
				  usecols=(0,1,2,3,4,5,6,7,8,9,10),
				  missing_values={8:" ", 'b':" ", 10:"N/A"},
				  filling_values={8:0.0, 'b':0, 10:0.0},
				  autostrip=True)
	try:
		x=np.recfromcsv(dfile,missing_values={8:" ",9:" ",'b':" ", 10:"N/A"},filling_values={8:0.0,9:"",'b':0, 10:0.0}) #, **kwargs)
		print(s[4],s[5])
		dsetname=str()
#USe symbols, since symbol NAN is considered as a nan a floating point number in recvfromcsv call
		dsetname= bytes.decode(s[4])
#this means it is for hedging purpose only
		if(bytes.decode(s[5]) != "N/A"): 
			dsetname += bytes.decode(s[5])
		print(dsetname)
		category=x[1]['category']
		category_txt = category.decode(encoding='windows-1252')
		print(category_txt)
		grp_name = "yahoo/HP/"
		grp_name += category_txt
		print(grp_name)
		group = f.require_group(grp_name)
		dset=group.create_dataset(dsetname,data=x) #x.shape,x.dtype,x.data)
		print(list(f))
		print(dset.name)
	except IOError as e:
		print("Could not find " + fname)
		continue
sfile ="P:\\FSB\\CEF\\data\\STK_Symbols.csv"
symbols=np.recfromcsv(sfile)
for s in symbols:
	dfile="P:\FSB\CEF\\data\\"
	fname = bytes.decode(s[0]) + ".csv"
	dfile+=fname
	try:
		x=np.recfromcsv(dfile,missing_values={8:" ", 'b':" ", 10:"N/A"},filling_values={8:0.0, 'b':0, 10:0.0}) #, **kwargs)
		print(x[1]['symbol'])
		dsetname=str()
		dsetname= bytes.decode(x[1]['symbol'])
		print(dsetname)
		category=x[1]['Category']
		category_txt = category.decode(encoding='windows-1252')
		print(category_txt)
		grp_name = "yahoo/HP/"
		grp_name += category_txt
		print(grp_name)
		group = f.require_group(grp_name)
		dset=group.create_dataset(dsetname,data=x) #x.shape,x.dtype,x.data)
		print(list(f))
		print(dset.name)
	except IOError as e:
		print("Could not find " + fname)
		continue
f.close()
# f1=h5py.File("P:\\FSB\\CEF\\hdf5\\cef_download.hdf5","r")
# dset2=f1['/yahoo/HP/CEF_EQUITY/USAXUSAX']
# st_date='2005-01-01' #dt.datetime.strptime('2005-01-01','%Y-%m-%d')
# print(bytes.decode(dset2[0]['date']),st_date)
# newdset= np.array([a_row for a_row in dset2 if st_date < bytes.decode(a_row['date'])],dtype=dset2.dtype)
# print(newdset)
# print(newdset.dtype)
# print(newdset[2]['date'],dset2[2]['close'])
# print(newdset['date'],newdset['close'],newdset['price'],",")
# basis=((newdset['close']/newdset['price']) -1)*100
# print(basis)
# f1.close()
#print(len(x))
#print(x.dtype)
#print(x.ndim)
#print(type(x))
#print(x)
#print(x[2][1],x[2][5]) //accessing by value
#print(np.std(x.divs,dtype=np.float64))