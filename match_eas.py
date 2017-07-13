import sys
import googlemaps
from time import perf_counter
sys.setrecursionlimit(10000)
from fuzzywuzzy import process,fuzz
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import pickle
import numpy as np
import requests
import os
import math
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from sklearn.neighbors import BallTree
pd.set_option("display.max_columns",101)


def join_data_on_address_GPS(radius=40,df=None):

    reference=pd.read_csv('./raw_csv/Addresses_-_Enterprise_Addressing_System.csv')
    reference['LonRad'] = reference['Longitude'].apply(math.radians)
    reference['LatRad'] = reference['Latitude'].apply(math.radians)

    
    if 'Permit Address' in df.columns:
        df.rename(columns={'Permit Address':'Address'},inplace=True)


    # if 'Longitude' not in df.columns:
    #     if 'Location' in df.columns:
    #         df['Longitude']=df.Location.apply(lambda x: float(str(x[1:-1]).split(',')[1]) if not pd.isnull(x) else None)
    #
    #         df['Latitude']=df.Location.apply(lambda x: float(str(x[1:-1]).split(',')[0]) if not pd.isnull(x) else None)
    #     else:
    #
    #         print('Location column is missing')
    #
    #         exit()
    #df=df[['Address','Longitude','Latitude']]

    #df.drop_duplicates(subset=['Address','Longitude','Latitude'],inplace=True)


    class r_closest_EAS(BallTree):
        def __init__(self,reference=None,*args,**kwargs):
            self.reference=reference
            data=reference[['LonRad','LatRad']].values

            super(r_closest_EAS,self).__init__(data=data,*args,**kwargs)
        def search_around(self,lon=None,lat=None,address=None,radius=None):

            if (lon or lat or address) == None:
                print('Nothing found')
                return

            indices=self.query_radius(np.array([lon,lat]).reshape(1,-1),r=radius)
            indices=indices[0].tolist()
            found_places=self.reference.iloc[indices]
            found_addresses=found_places['Address'].values.tolist()

            if found_addresses ==[]:
                return
            closest_address,score=process.extractOne(query=address,choices=found_addresses)
            closest_index=found_addresses.index(closest_address)
            closest_place=found_places.iloc[closest_index]
            closest_eas=closest_place['EAS BaseID']
            if closest_eas is None:
                print('None found')
            return closest_eas

    df['LonRad']=df['Longitude'].apply(math.radians)
    df['LatRad'] = df['Latitude'].apply(math.radians)
    r_radians = radius / 40075000 * 2 * math.pi * .7
    k=r_closest_EAS(reference=reference,metric='haversine')
    i=df#[df.index==108] #274171]

    #i=i.sample(5,replace=False)

    eas_match=i.apply(lambda cols:k.search_around(cols['LonRad'],cols['LatRad'],cols['Address'],radius=r_radians),axis=1)
    # except:
    #     print(i[i.index==81])
    #


    # i.drop(['LonRad','LatRad'],axis=1,inplace=True)


    return eas_match
