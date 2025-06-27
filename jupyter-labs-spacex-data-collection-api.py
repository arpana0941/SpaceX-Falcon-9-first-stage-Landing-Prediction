#!/usr/bin/env python
# coding: utf-8

# <p style="text-align:center">
#     <a href="https://skills.network" target="_blank">
#     <img src="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/assets/logos/SN_web_lightmode.png" width="200" alt="Skills Network Logo">
#     </a>
# </p>
# 

# # **SpaceX  Falcon 9 first stage Landing Prediction**
# 

# # Lab 1: Collecting the data
# 

# Estimated time needed: **45** minutes
# 

# In this capstone, we will predict if the Falcon 9 first stage will land successfully. SpaceX advertises Falcon 9 rocket launches on its website with a cost of 62 million dollars; other providers cost upward of 165 million dollars each, much of the savings is because SpaceX can reuse the first stage. Therefore if we can determine if the first stage will land, we can determine the cost of a launch. This information can be used if an alternate company wants to bid against SpaceX for a rocket launch. In this lab, you will collect and make sure the data is in the correct format from an API. The following is an example of a successful and launch.
# 

# ![](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DS0701EN-SkillsNetwork/lab_v2/images/landing_1.gif)
# 

# Several examples of an unsuccessful landing are shown here:
# 

# ![](https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-DS0701EN-SkillsNetwork/lab_v2/images/crash.gif)
# 

# Most unsuccessful landings are planned. Space X performs a controlled landing in the oceans. 
# 

# ## Objectives
# 

# In this lab, you will make a get request to the SpaceX API. You will also do some basic data wrangling and formating. 
# 
# - Request to the SpaceX API
# - Clean the requested data
# 

# ----
# 

# ## Import Libraries and Define Auxiliary Functions
# 

# We will import the following libraries into the lab
# 

# In[1]:


# Requests allows us to make HTTP requests which we will use to get data from an API
import requests
# Pandas is a software library written for the Python programming language for data manipulation and analysis.
import pandas as pd
# NumPy is a library for the Python programming language, adding support for large, multi-dimensional arrays and matrices, along with a large collection of high-level mathematical functions to operate on these arrays
import numpy as np
# Datetime is a library that allows us to represent dates
import datetime

# Setting this option will print all collumns of a dataframe
pd.set_option('display.max_columns', None)
# Setting this option will print all of the data in a feature
pd.set_option('display.max_colwidth', None)


# Below we will define a series of helper functions that will help us use the API to extract information using identification numbers in the launch data.
# 
# From the <code>rocket</code> column we would like to learn the booster name.
# 

# In[2]:


# Takes the dataset and uses the rocket column to call the API and append the data to the list
def getBoosterVersion(data):
    for x in data['rocket']:
       if x:
        response = requests.get("https://api.spacexdata.com/v4/rockets/"+str(x)).json()
        BoosterVersion.append(response['name'])


# From the <code>launchpad</code> we would like to know the name of the launch site being used, the logitude, and the latitude.
# 

# In[3]:


# Takes the dataset and uses the launchpad column to call the API and append the data to the list
def getLaunchSite(data):
    for x in data['launchpad']:
       if x:
         response = requests.get("https://api.spacexdata.com/v4/launchpads/"+str(x)).json()
         Longitude.append(response['longitude'])
         Latitude.append(response['latitude'])
         LaunchSite.append(response['name'])


# From the <code>payload</code> we would like to learn the mass of the payload and the orbit that it is going to.
# 

# In[4]:


# Takes the dataset and uses the payloads column to call the API and append the data to the lists
def getPayloadData(data):
    for load in data['payloads']:
       if load:
        response = requests.get("https://api.spacexdata.com/v4/payloads/"+load).json()
        PayloadMass.append(response['mass_kg'])
        Orbit.append(response['orbit'])


# From <code>cores</code> we would like to learn the outcome of the landing, the type of the landing, number of flights with that core, whether gridfins were used, wheter the core is reused, wheter legs were used, the landing pad used, the block of the core which is a number used to seperate version of cores, the number of times this specific core has been reused, and the serial of the core.
# 

# In[5]:


# Takes the dataset and uses the cores column to call the API and append the data to the lists
def getCoreData(data):
    for core in data['cores']:
            if core['core'] != None:
                response = requests.get("https://api.spacexdata.com/v4/cores/"+core['core']).json()
                Block.append(response['block'])
                ReusedCount.append(response['reuse_count'])
                Serial.append(response['serial'])
            else:
                Block.append(None)
                ReusedCount.append(None)
                Serial.append(None)
            Outcome.append(str(core['landing_success'])+' '+str(core['landing_type']))
            Flights.append(core['flight'])
            GridFins.append(core['gridfins'])
            Reused.append(core['reused'])
            Legs.append(core['legs'])
            LandingPad.append(core['landpad'])


# Now let's start requesting rocket launch data from SpaceX API with the following URL:
# 

# In[6]:


spacex_url="https://api.spacexdata.com/v4/launches/past"


# In[7]:


response = requests.get(spacex_url)


# Check the content of the response
# 

# In[29]:


#print(response.content)


# You should see the response contains massive information about SpaceX launches. Next, let's try to discover some more relevant information for this project.
# 

# ### Task 1: Request and parse the SpaceX launch data using the GET request
# 

# To make the requested JSON results more consistent, we will use the following static response object for this project:
# 

# In[8]:


static_json_url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/API_call_spacex_api.json'


# We should see that the request was successfull with the 200 status response code
# 

# In[9]:


response=requests.get(static_json_url)


# In[10]:


response.status_code


# Now we decode the response content as a Json using <code>.json()</code> and turn it into a Pandas dataframe using <code>.json_normalize()</code>
# 

# In[11]:


# Use json_normalize meethod to convert the json result into a dataframe
data = response.json()  # <- this is the fix: CALL the function
df = pd.json_normalize(data)


# Using the dataframe <code>data</code> print the first 5 rows
# 

# In[12]:


# Get the head of the dataframe
print(df.head())


# In[13]:


import pandas as pd
import requests
import datetime

# Step 1: Fetch and normalize the data
static_json_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/API_call_spacex_api.json'
response = requests.get(static_json_url)
data = response.json()
df = pd.json_normalize(data)

# Step 2: Work with the DataFrame, not the list
df = df[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]

# Step 3: Remove rows with multiple cores or payloads
df = df[df['cores'].map(len) == 1]
df = df[df['payloads'].map(len) == 1]

# Step 4: Flatten the list of 1 element
df['cores'] = df['cores'].map(lambda x: x[0])
df['payloads'] = df['payloads'].map(lambda x: x[0])

# Step 5: Convert date_utc to date
df['date'] = pd.to_datetime(df['date_utc']).dt.date

# Step 6: Filter by date
df = df[df['date'] <= datetime.date(2020, 11, 13)]

# Your clean `data` DataFrame is now ready
data = df
df


# You will notice that a lot of the data are IDs. For example the rocket column has no information about the rocket just an identification number.
# 
# We will now use the API again to get information about the launches using the IDs given for each launch. Specifically we will be using columns <code>rocket</code>, <code>payloads</code>, <code>launchpad</code>, and <code>cores</code>.
# 

# * From the <code>rocket</code> we would like to learn the booster name
# 
# * From the <code>payload</code> we would like to learn the mass of the payload and the orbit that it is going to
# 
# * From the <code>launchpad</code> we would like to know the name of the launch site being used, the longitude, and the latitude.
# 
# * **From <code>cores</code> we would like to learn the outcome of the landing, the type of the landing, number of flights with that core, whether gridfins were used, whether the core is reused, whether legs were used, the landing pad used, the block of the core which is a number used to seperate version of cores, the number of times this specific core has been reused, and the serial of the core.**
# 
# The data from these requests will be stored in lists and will be used to create a new dataframe.
# 

# In[14]:


#Global variables 
BoosterVersion = []
PayloadMass = []
Orbit = []
LaunchSite = []
Outcome = []
Flights = []
GridFins = []
Reused = []
Legs = []
LandingPad = []
Block = []
ReusedCount = []
Serial = []
Longitude = []
Latitude = []


# These functions will apply the outputs globally to the above variables. Let's take a looks at <code>BoosterVersion</code> variable. Before we apply  <code>getBoosterVersion</code> the list is empty:
# 

# In[15]:


BoosterVersion


# Now, let's apply <code> getBoosterVersion</code> function method to get the booster version
# 

# In[16]:


# Call getBoosterVersion
getBoosterVersion(data)


# the list has now been update 
# 

# In[17]:


BoosterVersion[0:5]


# we can apply the rest of the  functions here:
# 

# In[18]:


# Call getLaunchSite
getLaunchSite(data)


# In[19]:


# Call getPayloadData
getPayloadData(data)


# In[20]:


# Call getCoreData
getCoreData(data)


# Finally lets construct our dataset using the data we have obtained. We we combine the columns into a dictionary.
# 

# In[21]:


launch_dict = {'FlightNumber': list(data['flight_number']),
'Date': list(data['date']),
'BoosterVersion':BoosterVersion,
'PayloadMass':PayloadMass,
'Orbit':Orbit,
'LaunchSite':LaunchSite,
'Outcome':Outcome,
'Flights':Flights,
'GridFins':GridFins,
'Reused':Reused,
'Legs':Legs,
'LandingPad':LandingPad,
'Block':Block,
'ReusedCount':ReusedCount,
'Serial':Serial,
'Longitude': Longitude,
'Latitude': Latitude}


# In[22]:


for key, value in launch_dict.items():
    print(f"{key}: {len(value)}")


# Then, we need to create a Pandas data frame from the dictionary launch_dict.
# 

# In[23]:


# Create a data from launch_dict
# Step 2: Convert to a DataFrame
final_df = pd.DataFrame(launch_dict)

# Step 3: Display summary and head
print("Summary of DataFrame:")
print(final_df.info())  # shows data types and non-null counts

print("\nFirst 5 rows of DataFrame:")
print(final_df.head())


# Show the summary of the dataframe
# 

# In[ ]:


# Show the head of the dataframe


# In[26]:


print(df.columns)


# ### Task 2: Filter the dataframe to only include `Falcon 9` launches
# 

# Finally we will remove the Falcon 1 launches keeping only the Falcon 9 launches. Filter the data dataframe using the <code>BoosterVersion</code> column to only keep the Falcon 9 launches. Save the filtered data to a new dataframe called <code>data_falcon9</code>.
# 

# In[27]:


import pandas as pd
import requests

# Step 1: Get rocket details from SpaceX API
rockets_url = 'https://api.spacexdata.com/v4/rockets'
rockets_data = requests.get(rockets_url).json()

# Step 2: Convert to DataFrame
rockets_df = pd.json_normalize(rockets_data)[['id', 'name']]
rockets_df.rename(columns={'id': 'rocket', 'name': 'BoosterVersion'}, inplace=True)

# Step 3: Merge rocket names into your launch dataframe (df)
df = df.merge(rockets_df, on='rocket', how='left')

# Step 4: Filter only Falcon 9 launches
data_falcon9 = df[df['BoosterVersion'] == 'Falcon 9'].copy()

# Step 5: Reset the FlightNumber column
data_falcon9.loc[:, 'FlightNumber'] = list(range(1, data_falcon9.shape[0] + 1))

# Step 6: Preview
data_falcon9.head()



# Now that we have removed some values we should reset the FlgihtNumber column
# 

# In[28]:


data_falcon9.loc[:,'FlightNumber'] = list(range(1, data_falcon9.shape[0]+1))
data_falcon9


# ## Data Wrangling
# 

# We can see below that some of the rows are missing values in our dataset.
# 

# In[29]:


data_falcon9.isnull().sum()


# Before we can continue we must deal with these missing values. The <code>LandingPad</code> column will retain None values to represent when landing pads were not used.
# 

# ### Task 3: Dealing with Missing Values
# 

# Calculate below the mean for the <code>PayloadMass</code> using the <code>.mean()</code>. Then use the mean and the <code>.replace()</code> function to replace `np.nan` values in the data with the mean you calculated.
# 

# In[31]:


# Calculate the mean value of PayloadMass column
import pandas as pd
import requests

# Step 1: Get detailed payload data
payloads_url = 'https://api.spacexdata.com/v4/payloads'
payloads_data = requests.get(payloads_url).json()

# Step 2: Convert to DataFrame
payloads_df = pd.json_normalize(payloads_data)[['id', 'mass_kg']]
payloads_df.rename(columns={'id': 'payload_id', 'mass_kg': 'payload_mass_kg'}, inplace=True)

# Step 3: Expand payload ID from df (assumes one payload per launch for simplicity)
df['payload_id'] = df['payloads'].apply(lambda x: x[0] if isinstance(x, list) and x else None)

# Step 4: Merge payload mass into main df
df = df.merge(payloads_df, how='left', left_on='payload_id', right_on='payload_id')

# Step 5: Fill missing payload mass with the column mean
payload_mean = df['payload_mass_kg'].mean()
df['payload_mass_kg'].fillna(payload_mean, inplace=True)

# Step 6: Confirm missing values are gone
print("Missing values in payload_mass_kg:", df['payload_mass_kg'].isna().sum())



# You should see the number of missing values of the <code>PayLoadMass</code> change to zero.
# 

# Now we should have no missing values in our dataset except for in <code>LandingPad</code>.
# 

# We can now export it to a <b>CSV</b> for the next section,but to make the answers consistent, in the next lab we will provide data in a pre-selected date range. 
# 

# <code>data_falcon9.to_csv('dataset_part_1.csv', index=False)</code>
# 

# ## Authors
# 

# <a href="https://www.linkedin.com/in/joseph-s-50398b136/">Joseph Santarcangelo</a> has a PhD in Electrical Engineering, his research focused on using machine learning, signal processing, and computer vision to determine how videos impact human cognition. Joseph has been working for IBM since he completed his PhD. 
# 

# <!--## Change Log
# -->
# 

# <!--
# 
# |Date (YYYY-MM-DD)|Version|Changed By|Change Description|
# |-|-|-|-|
# |2020-09-20|1.1|Joseph|get result each time you run|
# |2020-09-20|1.1|Azim |Created Part 1 Lab using SpaceX API|
# |2020-09-20|1.0|Joseph |Modified Multiple Areas|
# -->
# 

# Copyright ©IBM Corporation. All rights reserved.
# 
