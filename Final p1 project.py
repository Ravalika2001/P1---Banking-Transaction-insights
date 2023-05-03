#!/usr/bin/env python
# coding: utf-8

# In[4]:


import findspark
findspark.init()
from pyspark.sql import *
import matplotlib.pyplot as plt
from pyspark import StorageLevel
import pandas as pd
import seaborn as sns
spark = SparkSession.builder.getOrCreate()
def printall():
    df.show()
    df.count()
def limitedrows():
    c = int(input('Enter number of rows to be disaplyed'))
    print(df.take(c))
       
def sortingdataset():
    e=df1.map(lambda x: x.split(','))
    c=e.map(lambda x: (x[1],x[6])).sortByKey(0).take(10)  
    d=e.map(lambda x: (x[1],x[0])).sortByKey(0).take(10)
    print("After sorting the data")
    print(c)
    print(d)

def Eligiblecustomers():
    f=df.filter(df["CreditScore"]>700)
    print(f.count())
   
def genderdistinct():
    df.select("Marital Status").distinct().show()
         
def filteringloandataset():
    print('counting of Loan Amount who is having greater than 1000000')
    c=df.filter(df["Loan Amount"]>"1,00,000")
    print(c.count())
    print('number of people with 2 or more returned cheques and are single')
    d=df.filter((df[" Returned Cheque"]>"1") & (df["Marital Status"]<"SINGLE"))
    print(d.count())
    print('number of people with expenditure over 50000 a month ')
    df.filter((df["Expenditure"]>"50000")).show()
   
def filteringcreditcarddataset():
    print('number of members who are  elgible and active in the bank:',end = " ")
    c=df.filter((df["IsActiveMember"]==1) & (df["CreditScore"]>700))
    print(c)
    print('credit card users in France ')
    df.filter(df['Geography'] == "France").show()
           
       
def partioningofdataonloancategory():
    df.groupBy("Loan Category").count().orderBy("count", ascending = False).show()
   
def partioningofdataoncreditcards():
    df.groupby('Geography').count().orderBy("count",ascending = True).show()

   
def agegroup():
    age = int(input('Enter an age to get data of particular age people'))
    c = df.filter((df["Age"] == age))
    if c.count()>0:
        df.select("Customer_ID", "Age", "Occupation", "Marital Status", "Income").filter(df.Age == age).show()
    else:
        print('The entered  age group is not present in dataset')
   
def visulizecreditcard():
    pd1=df.toPandas()
    plt.figure(figsize=(3,2))
    sns.barplot(x='Geography',y='Age', data=pd1)
    plt.title("Geograpy vs age")
   
   
def visulizeloandata():
    pd1=df.toPandas()
    plt.figure(figsize=(5,5))
    sns.barplot(x='Family Size',y='Loan Category', data=pd1)
    plt.title("Family Size  vs Loan Category")
   
   
def persistloan():
    df.persist(StorageLevel.MEMORY_ONLY)
    result = df.filter(df['Loan Category'] == "GOLD LOAN").show()
    df.unpersist()
   
def persistcredit():
    df.persist(StorageLevel.DISK_ONLY)
    result = df.filter(df['Geography'] == "France").show()
    df.unpersist()

choose =int(input('Enter a number to choose 1 to work with loan dataset 2 to work with creditcard dataset'))
if choose == 1:
    while True:
        df = spark.read.csv("C:\\Users\\Ravalika\\Desktop\\loan.csv",inferSchema=True,header=True)
        df1 = spark.sparkContext.textFile("C:\\Users\\Ravalika\\Desktop\\loan.csv")
        print('---1.to view  complete dataset--- ')
        print('---2.to view limited rows as per user required---')
        print('---3.to perfrom sorting operation on dataset---')
        print('---4.to perfrom filter operation on dataset---')
        print('---5.to perform Partitioning dataset on columns---')
        print('--6.to diaplay data of particular age group---')
        print('---7.To persist data---')
        print('---8.To disaply distinct gender---')
        print('---9.to visualizedata---')
        print('---Enter any number to exit---')
        choice = int(input('Enter a number to choose to perfrom an action :'))
        if choice == 1:
            printall()
            print('To show complete data set')
        elif choice == 2:
            print('To show limited rows')
            limitedrows()
        elif choice == 3:
            sortingdataset()
            print('To perform sort operation on dataset')
        elif choice == 4:
            filteringloandataset()
            print('To filter dataset')
        elif choice == 5:
            partioningofdataonloancategory()
            print('Partitioning dataframe on column ‘Loan Category’')
        elif choice == 6:
            print('To display of particular age group')
            agegroup()
        elif choice == 7:
            print('To persist data')
            persistloan()
        elif choice == 8:
            print('To disaply distinct gender')
            genderdistinct()
        elif choice == 9:
            print('To visulize data')
            visulizeloandata()
        else:
            print('The number you enterted may not exist or please choose in range between from 1 and 7')
            break
elif choose == 2:
    while True:
        print('---1 to view  complete dataset--- ')
        print('---2 to view limited rows as per user required---')
        print('---3 to perfrom sorting operation on dataset---')
        print('---4.to perfrom filter operation on dataset---')
        print('---5.to perform Partitioning dataset on columns---')
        print('---6.to persist data---')
        print('---7.to display who are eligible for credit card---')
        print('---8.to visulize dataset---')
        print('---Enter any number to exit---')
        choice =int(input('Enter a number to choose to perform operation'))
        df = spark.read.csv("C:\\Users\\Ravalika\\Downloads\\PROJECT2-main\\PROJECT2-main\\DATASETS\\credit card.csv",inferSchema = True,header=True)
        df1 = spark.sparkContext.textFile("C:\\Users\\Ravalika\\Downloads\\PROJECT2-main\\PROJECT2-main\\DATASETS\\credit card.csv")
       
        if choice == 1:
            print('To show complete dataset')
            printall()
        elif choice == 2:
            print('To display limited rows according to user')
            limitedrows()
        elif choice == 3:
            print('Sorting of data')
            sortingdataset()
        elif choice == 4:
            print('Filtering of data')
            filteringcreditcarddataset()
        elif choice == 5:
            print('Partioiongof data according to particular loan categoery')
            partioningofdataoncreditcards()
        elif choice == 6:
            print('To persist data')
            persistcredit()
        elif choice == 7:
            print('To display who are eligibl for creditcards')
            Eligiblecustomers()
        elif choice == 8:
            print('TO visulize the data')
            visulizecreditcard()
        else:
            print('The enterd number does not exist please check the number')
            break


# In[ ]:




