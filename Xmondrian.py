import pandas as pd
import statistics as st
import time
df=pd.read_csv(r'C:\Users\chathura\Desktop\CHANDU\py_freq\data1.csv')
print(df.head())
col=input("Enter the Quasi Identifiers like 'Age','Education',....etc ")
SA=input("Enter the Sensitive Attribute ")
col1=list(col.split(' '))
QI_List=list(col.split(' '))
col1.append(SA)
t=30
data=df[col1].head(t)
#t=list(df.shape)[0]
def dis(data,k_index):
	no_dis=False
	l=list(data.keys())
	d={}
	y=0
	for i in l:
		x=list(data[i])
		l1=len(set(x))
		if(l1==1 or l1<k_index or len(x)<=((2*k_index)-1)):
			no_dis=True
			continue
		else:
			d[str(l1)+str(y)]=[x,i]
			y=y+1
	d1={}
	k=list(d.keys())
	for i in range (len(k)):
		k[i]=int(k[i])
	k=sorted(k)
	for i in range(len(k)):
		d1.update({k[i]:d[str(k[i])]})
	return(d1,len(d1))
def frequency(l,data):
	l1=[]
	l2=[]
	l3=[]
	d={}
	for i in l:
		l1.append(l.count(i))
		d[i]=l.count(i)
	l1=list(d.values())
	med=st.median(l1)
	for i in range(len(l)):
		if (d[l[i]]>=med):
			l2.append(list(data.iloc[i,:]))
		else:
			l3.append(list(data.iloc[i,:]))
	df1=pd.DataFrame(l2)
	df2=pd.DataFrame(l3)
	return(df1,df2)
def find_attribute(data,k_index,w,flag=False):
	d,l1=dis(data,k_index)
	k=list(d.keys())
	y=0
	while(y<len(k)):
		if(int(str(k[y])[:-1])>=k_index):
			break
		else:
			y=y+1
	if(flag==False):
		if(l1!=0):
			return(y,d[k[y]],l1)
		else:
			return(0,[[9],[8]],0)
	elif(flag ==True):
		if(y<len(k)-1):
			return(y+1,d[k[y+1]],l1)
		else:
			return(0,[['Terminated'],[8]],0)
def grouping(data,k_index,l=[],w=0,flagg=False):
	if (flagg==False):
		y,at,l1=find_attribute(data,k_index,w)
	else:
		y,at,l1=find_attribute(data,k_index,w,flag=True)
	if (at[0][0]=='Terminated'):
		if (w<len(l)):
			grouping(l[w],k_index,w=w+1)        
		else:
			l=l
	else:
		if (l1!=0):
			s=list(set(at[0]))
			group_df=data.groupby(at[1])
			t=type(s[0])
			if (t==str):
				for i in range (len(s)):
					ele=group_df.get_group(s[i])
					l.append(ele)
			elif (t==int or t==float):
				ele1,ele2=frequency(at[0],data)
				if (len(ele2)):
					l.append(ele1)
					l.append(ele2)
				else:
					grouping(ele1,k_index,w=w,flagg=True)
		if(w<len(l)):
			sh=l[w].shape
			grouping(l[w],k_index,w=w+1)        
	return l
def generalize(data,QI_List):
    l=0
    i=0
    for x in QI_List:
        att=x
        demo=data[att].unique()
        demo1=len(demo)
        attlist=data[att].unique()
        att2=attlist
        o=0
        while demo1>1:
            att1=str(att)+'.csv'
            gen=pd.read_csv(att1)
            attlist=data[att].unique()
            j=0
            for y in attlist:
                e=att2[j]
                n='level'+str(o)
                s='level0'
                z=gen.set_index(s).index.get_loc(e)
                f=gen.at[z,n]
                data[att].replace(y,f, inplace=True)
                j=j+1
            demo=data[att].unique()
            demo1=len(demo)
            o=o+1
            i=i+1
    return data

k_index=int(input("Enter the k_value"))
start=time.time()
l=grouping(data,k_index)
l1=[]
l2=[]
for i in l:
	i.columns=col1
	if (list(i.shape)[0]<=(2*k_index-1)):
		l1.append(i)
print("the final partitions are")
for j in l1:
	print(j)
	print("------------------------------------------")
for r in l1:
	d1=generalize(r,QI_List)
	d1.columns=col1
	l2.append(d1)
df=pd.concat(l2)
print("the generalized list")
print(df)
end=time.time()
dm=0
for i in l1:
	s=list(i.shape)
	if (s[0]>=k_index):
		dm=dm+s[0]*s[0]
	else:
		dm=dm+s[0]*t
cavg=t/(k_index*len(l1))
print("The Discernability Metric is ",dm)
print("The C Average is ",cavg)
print("The execuion time is ",end-start)
