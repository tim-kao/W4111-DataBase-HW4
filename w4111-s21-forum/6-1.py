country_to_regions = [('Australia', 'APAC'),
 ('Austria', 'EMEA'),
 ('Belgium', 'EMEA'),
 ('Canada', 'NA'),
 ('Denmark', 'EMEA'),
 ('Finland', 'EMEA'),
 ('France', 'EMEA'),
 ('Germany', 'EMEA'),
 ('Hong Kong', 'APAC'),
 ('Ireland', 'EMEA'),
 ('Israel', 'EMEA'),
 ('Italy', 'EMEA'),
 ('Japan', 'APAC'),
 ('Netherlands', 'EMEA'),
 ('New Zealand', 'APAC'),
 ('Norway', 'EMEA'),
 ('Philippines', 'APAC'),
 ('Poland', 'EMEA'),
 ('Portugal', 'EMEA'),
 ('Russia', 'EMEA'),
 ('Singapore', 'APAC'),
 ('South Africa', 'EMEA'),
 ('Spain', 'EMEA'),
 ('Sweden', 'EMEA'),
 ('Switzerland', 'EMEA'),
 ('UK', 'EMEA'),
 ('USA', 'NA')]

from pymongo import MongoClient
import pandas as pd
client = MongoClient("mongodb://localhost:27017/admin?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
#select database
customers = client['classic_models']['customers']
#select the collection within the database

for item in country_to_regions:
    res = customers.update_many({'country': item[0]}, {'$set': {'region': item[1]}})
    print(res)
