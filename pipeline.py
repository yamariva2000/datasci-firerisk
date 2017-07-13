# df.apply(lambda rows:len(rows.unique()),axis=0)
import Tax_Rolls
import pandas as pd
import os
import numpy as np
import match_eas
pd.set_option('display.max_columns', 1000)


def convert_location_to_lon_lat(string=None):
    try:
        return [float(i) for i in string.strip('()').split(',')]
    except:
        return None


def main(limit_rows=None):

    path='raw_csv'
    files=sorted(os.listdir(os.path.join(path)))


    special_functions={'Buyout_agreements.csv':{'Buyout Amount':sum,'Number of Tenants':sum},
            'Fire_Incidents.csv':{'Estimated Property Loss':sum,
           'Estimated Contents Loss':sum, 'Fire Fatalities':sum, 'Fire Injuries':sum,
           'Civilian Fatalities':sum, 'Civilian Injuries':sum}}

    special_filters={'Fire_Incidents.csv':{'Primary Situation':lambda x: x[0]=='1'}}
    conversions={'Buyout_agreements.csv':{'Buyout Amount':lambda x: pd.to_numeric(x,errors='coerce')}}

    date_overrides={}

    files=files[1:]


    dateparser=lambda x:pd.datetime.strptime(x,'%m/%d/%Y') if isinstance(x,str) else x

    for file in files:
        print(file)
        df=pd.read_csv(os.path.join(path,file),nrows=limit_rows,)
        print(df.dtypes)
        print(df.columns)
        if file in special_filters:
            for key,function in special_filters[file].items():
                mask=df[key].apply(function)
                df=df[mask]
        if file in conversions:
            for key,conversion in conversions[file].items():
                df[key]=df[key].apply(conversion)

        if 'Location' in df.columns:
            df['Location_List']=df['Location'].apply(lambda x:convert_location_to_lon_lat(x))
            df['Longitude']=df['Location_List'].apply(lambda x:x[1] if x else None)
            df['Latitude'] = df['Location_List'].apply(lambda x: x[0] if x else None)

        date_columns=[]

        for column in df.columns:
            if 'Date' in column :
                date_columns.append(column)
                try:
                    df[column]=df[column].apply(dateparser)
                    df[column]=df[column].apply(lambda x:x.year if isinstance(x,pd.datetime) else x)
                except:
                    date_columns.pop()

        date_split=pd.datetime(2014,1,1)

        #print(df.duplicated('Complaint Number').sum())
        if 'Address' not in df.columns:
            df['Address']=df.apply(lambda cols:'{} {} {}'.format(cols['Street Number'],cols['Street Name'],cols['Street Suffix']),axis=1)

        #print(df.head())
        #print(df.groupby(['Address','Longitude','Latitude'])[date_columns].count().head())
        df['min']=df.apply(lambda cols:cols[date_columns].min(),axis=1)

        init=True
        for column in date_columns:

            pivot=pd.pivot_table(df,index=['Address','Longitude','Latitude','Location'],columns=column,dropna=True, fill_value=0,values='Lot',aggfunc='count')
            pivot.columns=['{}_{:.0f}'.format(column,i) for i in pivot.columns]
            if init:
                aggregate=pivot
                init=False
            else:
                aggregate=pd.concat([aggregate,pivot], axis=1,verify_integrity=True)
                aggregate.fillna(0,inplace=True)



        if file in special_functions:
            for column,function in special_functions[file].items():


                pivot = pd.pivot_table(df, index=['Address', 'Longitude', 'Latitude','Location'], dropna=True, columns='min',
                                       fill_value=0, values=column, aggfunc=function)
                pivot.columns=['{}_{:.0f}'.format(column,i) for i in pivot.columns]
                aggregate=pd.concat([aggregate,pivot],axis=1,verify_integrity=True)
                aggregate.fillna(0, inplace=True)
        aggregate=aggregate.reset_index()

        aggregate['EAS BaseID Matched']=match_eas.join_data_on_address_GPS(df=aggregate)


        aggregate.to_csv('./Processed/processed_'+file)


    Tax_Rolls.main(limit_rows=limit_rows)



if __name__=='__main__':
    main(limit_rows=None)