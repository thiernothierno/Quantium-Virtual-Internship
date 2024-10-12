#!/usr/bin/env python
# coding: utf-8

# ## Transaction  Data Exploration 

# In[ ]:


import pandas as pd
import numpy as np


# In[894]:


quantium = pd.read_excel("/Users/thiernodicko/Desktop/Panda_files/QVI_transaction_data.xlsx", "sheet1")


# In[895]:


# Printing the first 10 rows of the dataset
quantium


# In[896]:


# let view a summary of the dataset
quantium.describe()


# ## Anomalies: 

# First anomaly : Date column is in integer format, let change it to a date format

# In[897]:


quantium['DATE'] = pd.to_datetime(quantium['DATE'], unit='D', origin='1899-12-30')


# In[898]:


# Let show the format if it match to what we are looking for.
print(quantium['DATE'].dtype)


# In[899]:


# Printing the first 5 rows to see the update made.
quantium.head(5)


# ### Let examine the PROD_NAME column 

# 1- Since we are only looking at potato chips, let extract all digits from the prod_name column and create a new column to store them.

# In[900]:


# Extract all digits and storing them to a variable called PACK_SIZE
PACK_SIZE = quantium['PROD_NAME'].str.extractall(r'(\d+)')


# In[901]:


# Viewing the contain of the variable PACK_SIZE
PACK_SIZE.head(5)


# We noticed that this time serie contain multiple index level, let drop one level before adding this column to our dataset

# In[902]:


# Dropping the first level
quantium['PACK_SIZE'] = PACK_SIZE.droplevel(1)


# In[903]:


# first 5 rows of the dataset
quantium.head(5)


# Extracting the unit from the PROD_NAME column. 

# In[904]:


units = quantium['PROD_NAME'].str.extractall(r'(\d+)(\w+)')


# In[905]:


# dropping the first level of the units dataframe
units = units.droplevel([1])


# In[906]:


# Extracting the first column from the data
units = units.drop(0, axis=1)


# In[907]:


# Showing the first 5 row of the data
units.head(5)


# Let make sure that all units have the same symbol, if not, we can use the mode fuction to get the most frequent units.

# In[908]:


units.mode()


# In[909]:


units.at[0,1]


# Append this unit to our PACK_SIZE column of the dataset

# In[910]:


quantium = quantium.rename(columns={'PACK_SIZE': 'PACK_SIZE' + '_' + units.at[0,1]})


# In[911]:


# Checking if the update was successful
quantium.head(10)


# Now let remove all extracted digits and units from the PROD_NAME column and replace them by an empty space.

# In[912]:


quantium['PROD_NAME'] = quantium['PROD_NAME'].replace([r'(\d+)(\w+)'], '', regex=True)


# In[913]:


# Verifiying the update made
quantium.head(5)


# Removing all special characters from the prod_name column

# In[914]:


quantium['PROD_NAME'] = quantium['PROD_NAME'].replace(r'[^\w\s]', '', regex=True)


# Getting word from Prod_name column, splitting them into words, finding the frequency of each word and ordering them by their frequency from highest to lowest frequency. 

# In[915]:


# collecting word from the prod_name column
product_words = merged_data['PROD_NAME']


# In[916]:


# Creating a time serie
product_words = pd.Series(' '.join(product_words).split())


# In[917]:


# finding the frequency of each unique word
product_freq = pd.Series(product_words).value_counts().reset_index()


# In[918]:


# renaming columns
product_freq.columns = ['words','frequency']


# In[919]:


product_freq


# Since we are only interest in the chips category, let remove any salsa products from the dataset

# In[920]:


# creating a column Salsa that will return true if salsa appears in the product category, false otherwise
quantium['SALSA'] = quantium['PROD_NAME'].str.lower().str.contains('salsa')


# In[921]:


# Printing rows where the product salsa appears
quantium[quantium.SALSA == True]


# Remove all rows where the word "salsa" appears, then drop the column SALSA

# In[922]:


quantium = quantium[~quantium['SALSA']]


# In[923]:


# dropping the column SALSA
quantium = quantium.drop(columns='SALSA')


# In[924]:


# Checking update made
quantium.head(10)


# In[925]:


# Checking for null values
quantium.isnull().values.any()


# In[926]:


# Setting date column as the index column
quantium.set_index('DATE', inplace=True)


# In[927]:


# Summary of the dataset
quantium.describe()


# We noticed that there are not null values, however the product quatity column appears to have an outlier which we should investigate further.

# In[928]:


# Filter the product quantity column to find the outlier
quantium[quantium.PROD_QTY == 200]


# Let check if the above customer has made other transactions

# In[929]:


quantium[quantium.LYLTY_CARD_NBR == 226000]


# Its looks like this customer has only had the two transactions over the year and is not an ordinary retail customer.The customer might be bying chips for commercial purposes instead. Let remove this loyalty card number from the dataset.

# In[930]:


# Removing loyalty card number 226000 from the dataset
quantium = quantium[quantium['LYLTY_CARD_NBR'] != 226000]


# In[931]:


# Checking if the update was successful
quantium


# In[932]:


# Summary of the data
quantium.describe()


# In[933]:


# Counting the number of transaction by date
count_by_date = merged_data.groupby('DATE').size()


# In[934]:


count_by_date


# There's only 364 rows, meaning only 364 dates which indicates a missing date. Let's
# create a sequence of dates from 1 Jul 2018 to 30 Jun 2019 and use this to create a
# chart of number of transactions over time to find the missing date.

# In[935]:


# Sequence of date from 1 Jul 2018 to 30 Jun 2019
transaction_date = pd.date_range(start='2018-07-01', end='2019-06-30', freq='D')


# In[936]:


# Printing the dates
transaction_date


# Let reset the date index, sort the date, then show missing date

# In[937]:


# Resetting the date index.
quantium = quantium.reset_index()


# In[938]:


# Sorting the data by Date
quantium.sort_values(by='DATE')


# In[939]:


# showing missing date
transaction_date[~transaction_date.isin(quantium['DATE'])]


# We noticed that there is one missing date. Now let create a column of dates that includes every day from 1 Jul 2018 to 30 Jun 2019, and join it onto the data to fill in the missing day.

# In[940]:


# Creating a dataframe for the new sequence date created
complete_date = pd.DataFrame(transaction_date, columns=['DATE'])


# In[941]:


# Joining the sequence date with the existing date
quantium = complete_date.merge(quantium, on='DATE', how='left')


# In[942]:


# Verify if there still a missing date in the data
transaction_date[~transaction_date.isin(quantium['DATE'])]


# In[943]:


# Let verify if there are some null values
quantium.isnull().values.any()


# In[944]:


quantium[quantium.PROD_NAME.isnull()]


# We have a entire row with null value, let get ride of it. 

# In[945]:


quantium.dropna(inplace=True)


# In[946]:


# Rechecking for nan values.
quantium[quantium.PROD_NAME.isnull()]


# No missing date, no null values from our data. Now let move on to verify the output from the pack size we created earlier, and to creating other future such as brand of chips which can be done by extracting the first word of each row of the product name column.

# In[947]:


quantium.sort_values(by="PACK_SIZE_g")


# In[948]:


# Creating a column for the brand name.
quantium.insert(9, "BRAND_NAME",quantium['PROD_NAME'].str.split().str.get(0), True)


# In[949]:


quantium


# Let show all the unique entries of the brand name column

# In[954]:


quantium.BRAND_NAME.unique()


# Some of the brand names look like they are of the same brands, such as RED and RRD which are both Red Rock Deli Chips. Let's combine these together and make any additional brand adjustments.

# In[955]:


# Creating a function that will serve to join all brands with the same type
def replace_brandname(line):
    name = line['BRAND_NAME']
    if name == "Infzns":
        return "Infuzions"
    elif name == "Red":
        return "Red Rock Deli"
    elif name == "RRD":
        return "Red Rock Deli"
    elif name == "Grain":
        return "Grain Waves"
    elif name == "GrnWves":
        return "Grain Waves"
    elif name == "Snbts":
        return "Sunbites"
    elif name == "Natural":
        return "Natural Chip Co"
    elif name == "NCC":
        return "Natural Chip Co"
    elif name == "WW":
        return "Woolworths"
    elif name == "Smith":
        return "Smiths"
    elif name == "Dorito":
        return "Doritos"
    else:
        return name 


# In[958]:


# Then apply the function to clean the brand names
quantium["BRAND_NAME"] = quantium.apply(lambda line: replace_brandname(line), axis=1)


# In[959]:


# Verify the change made.
quantium.BRAND_NAME.unique()


# The above brand name seemed reasonable without duplicate.

# ### Customer Data Exploration

# Importing customer data 

# In[960]:


customer = pd.read_csv("/Users/thiernodicko/Desktop/Panda_files/QVI_purchase_behaviour.csv")


# In[961]:


# looking a the first 10 rows of the customer data
customer.head(10)


# In[962]:


# Check a summary of the customer data
customer.describe()


# In[963]:


# Check for null values in the dataset
customer.isnull().values.any()


# In[964]:


# Let rename the column 'PREMIUM_CUSTOMER' to 'MEMBER_TYPE' 

customer = customer.rename(columns={"PREMIUM_CUSTOMER" : "MEMBER_TYPE"})


# In[965]:


customer


# In[966]:


# Let check the entries of both column Lifestage and member_type in the dataset
customer['LIFESTAGE'].unique()


# In[967]:


customer['MEMBER_TYPE'].unique()


# The data looks fine: no null or missing values and no duplicate.

# Merging Transaction Data and Customer Data

# In[970]:


# Join the customer and transaction datasets, and sort transactons by date 
full_data = quantium.set_index('LYLTY_CARD_NBR').join(customer.set_index('LYLTY_CARD_NBR'))
full_data = full_data.reset_index()
full_data = full_data.sort_values(by='DATE').reset_index(drop=True)


# In[971]:


full_data


# In[972]:


# Checkin for null values.
full_data.isnull().values.any()


# The final data looks reasonable, hence let export it to csv file for further analysis.

# In[973]:


full_data.to_csv('QVI_finaldata.csv')


# 

# ### Next: I will use Power BI for data visualization and SQL to answer some business questions. 

# In[ ]:




