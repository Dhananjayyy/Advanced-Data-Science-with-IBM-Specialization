
# coding: utf-8

# # Assignment 3
# 
# Welcome to Assignment 3. This will be even more fun. Now we will calculate statistical measures on the test data you have created.
# 
# YOU ARE NOT ALLOWED TO USE ANY OTHER 3RD PARTY LIBRARIES LIKE PANDAS. PLEASE ONLY MODIFY CONTENT INSIDE THE FUNCTION SKELETONS
# Please read why: https://www.coursera.org/learn/exploring-visualizing-iot-data/discussions/weeks/3/threads/skjCbNgeEeapeQ5W6suLkA
# . Just make sure you hit the play button on each cell from top to down. There are seven functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# All functions can be implemented using DataFrames, ApacheSparkSQL or RDDs. We are only interested in the result. You are given the reference to the data frame in the "df" parameter and in case you want to use SQL just use the "spark" parameter which is a reference to the global SparkSession object. Finally if you want to use RDDs just use "df.rdd" for obtaining a reference to the underlying RDD object. 
# 
# Let's start with the first function. Please calculate the minimal temperature for the test data set you have created. We've provided a little skeleton for you in case you want to use SQL. You can use this skeleton for all subsequent functions. Everything can be implemented using SQL only if you like.

# In[6]:


def minTemperature(df,spark):
    return spark.sql("SELECT MIN(temperature) as mintemp from washing").first().mintemp


# Please now do the same for the mean of the temperature

# In[7]:


def meanTemperature(df,spark):
    return spark.sql("SELECT avg(temperature) as avgtemp from washing").first().avgtemp


# Please now do the same for the maximum of the temperature

# In[8]:


def maxTemperature(df,spark):
    return spark.sql("SELECT max(temperature) as maxtemp from washing").first().maxtemp


# Please now do the same for the standard deviation of the temperature

# In[9]:


def count1(df,spark):
        return spark.sql("SELECT count(frequency) as count1 from washing").first().count1


# In[10]:


def count2(df,spark):
        return spark.sql("SELECT count(*) as count2 from washing").first().count2


# In[11]:


def sdTemperature(df,spark):
    return spark.sql("SELECT std(temperature) as stdtemp from washing").first().stdtemp


# Please now do the same for the skew of the temperature. Since the SQL statement for this is a bit more complicated we've provided a skeleton for you. You have to insert custom code at four position in order to make the function work. Alternatively you can also remove everything and implement if on your own. Note that we are making use of two previously defined functions, so please make sure they are correct. Also note that we are making use of python's string formatting capabilitis where the results of the two function calls to "meanTemperature" and "sdTemperature" are inserted at the "%s" symbols in the SQL string.

# In[12]:


def skewTemperature(df,spark):    
    return spark.sql("""
SELECT 
    (
        count(temperature)/(count(temperature)-1)/(count(temperature)-2)
    ) *
    SUM (
        POWER(temperature-%s,3)/POWER(%s,3)
    )

as skewtemp from washing
                    """ %(meanTemperature(df,spark),sdTemperature(df,spark))).first().skewtemp


# Kurtosis is the 4th statistical moment, so if you are smart you can make use of the code for skew which is the 3rd statistical moment. Actually only two things are different.

# In[13]:


def kurtosisTemperature(df,spark):    
    return spark.sql("""
SELECT 
    (
        1/(count(temperature))
    ) *
    SUM (
        POWER(temperature-%s,4)/POWER(%s,4)
    )

as kurttemp from washing
                    """ %(meanTemperature(df,spark),sdTemperature(df,spark))).first().kurttemp


# Just a hint. This can be solved easily using SQL as well, but as shown in the lecture also using RDDs.

# In[14]:


def correlationTemperatureHardness(df,spark):
    return spark.sql("SELECT corr(temperature,hardness) as corrtemp from washing").first().corrtemp


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code
# 
# ### TODO Please provide your Cloudant credentials here

# In[15]:


### TODO Please provide your Cloudant credentials here by creating a connection to Cloudant and insert the code
# @hidden_cell
credentials_1 = {
  'password':"""ac157971aefa0787376ad08582ee9ec088901e5fb246b4e70f9fa624ed1ffd99""",
  'custom_url':'https://26560ce5-e11b-4035-a5f4-33df2699baf4-bluemix:ac157971aefa0787376ad08582ee9ec088901e5fb246b4e70f9fa624ed1ffd99@26560ce5-e11b-4035-a5f4-33df2699baf4-bluemix.cloudantnosqldb.appdomain.cloud',
  'username':'26560ce5-e11b-4035-a5f4-33df2699baf4-bluemix',
  'url':'https://undefined'
}
### Please have a look at the latest video "Connect to Cloudant/CouchDB from ApacheSpark in Watson Studio" on https://www.youtube.com/c/RomeoKienzler
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same


# In[16]:


#Please don't modify this function
def readDataFrameFromCloudant(database):
    cloudantdata=spark.read.load(database, "com.cloudant.spark")

    cloudantdata.createOrReplaceTempView("washing3")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# In[17]:


spark = SparkSession    .builder    .appName("Cloudant Spark SQL Example in Python using temp tables")    .config("cloudant.host",credentials_1['custom_url'].split(':')[2].split('@')[1])    .config("cloudant.username", credentials_1['username'])    .config("cloudant.password",credentials_1['password'])    .config("jsonstore.rdd.partitions", 1)    .getOrCreate()


# In[18]:


df=readDataFrameFromCloudant(database)


# In[19]:


minTemperature(df,spark)


# In[20]:


meanTemperature(df,spark)


# In[21]:


maxTemperature(df,spark)


# In[22]:


sdTemperature(df,spark)


# In[23]:


skewTemperature(df,spark)


# In[24]:


kurtosisTemperature(df,spark)


# In[25]:


correlationTemperatureHardness(df,spark)


# Congratulations, you are done, please download this notebook as python file using the export function and submit is to the gader using the filename "assignment3.1.py"
