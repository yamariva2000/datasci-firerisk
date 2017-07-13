import pandas as pd
import numpy as np
pd.set_option('display.max_rows', 1000)
"""Index(['Closed Roll Fiscal Year', 'Property Location', 'Neighborhood Code',
       'Neighborhood Code Definition', 'Block and Lot Number', 'Volume Number',
       'Property Class Code', 'Property Class Code Definition',
       'Year Property Built', 'Number of Bathrooms', 'Number of Bedrooms',
       'Number of Rooms', 'Number of Stories', 'Number of Units',
       'Characteristics Change Date', 'Zoning Code', 'Construction Type',
       'Lot Depth', 'Lot Frontage', 'Property Area in Square Feet',
       'Basement Area', 'Lot Area', 'Lot Code', 'Prior Sales Date',
       'Recordation Date', 'Document Number', 'Document Number 2',
       'Tax Rate Area Code', 'Percent of Ownership',
       'Closed Roll Exemption Type Code',
       'Closed Roll Exemption Type Code Definition', 'Closed Roll Status Code',
       'Closed Roll Misc Exemption Value',
       'Closed Roll Homeowner Exemption Value', 'Current Sales Date',
       'Closed Roll Assessed Fixtures Value',
       'Closed Roll Assessed Improvement Value',
       'Closed Roll Assessed Land Value',
       'Closed Roll Assessed Personal Prop Value', 'Supervisor District',
       'Neighborhoods - Analysis Boundaries', 'Location'],
      dtype='object')"""

"""Index(['EAS BaseID', 'CNN', 'Address', 'Address Number',
       'Address Number Suffix', 'Street Name', 'Street Type', 'Zipcode',
       'Longitude', 'Latitude', 'Location'],
      dtype='object')
"""

def parse_address(string=None):
    if isinstance(string,int):
        print(string)
        return
    string=string.strip()

    """"'Address Number',
    'Address Number Suffix', 'Street Name', 'Street Type'"""
    street_end_text=string[:4]
    if street_end_text.isdigit():
        street_end=int(street_end_text)
    else:
        street_end=0

    street_end_suffix = string[4].strip()

    street_start_text = string[5:9]
    if street_start_text.isdigit():
        street_start=int(street_start_text)
    else:
        street_start=0

    street_start_suffix=string[9].strip()

    if street_end==0:
        street_end=street_start

    rest=string[10:]

    street_name=rest[:-7].strip()

    street_type=rest[-7:-4].strip()[:2]
    street_etc=rest[-4:].strip()

    return street_start,street_start_suffix,street_end,street_end_suffix,street_name,street_type,street_etc

import time
def find(tuple,addresses):
    if tuple is None:
        return
    if tuple[5]:
        found = addresses[(addresses['Street Name'] == tuple[4]) & (addresses['Address Number'] >= tuple[0]) & (
        addresses['Address Number'] <= tuple[2])&(addresses['Street Type']==tuple[5][:2])]
        found = addresses[(addresses['Street Name'] == tuple[4]) & (addresses['Address Number'] >= tuple[0]) & (
        addresses['Address Number'] <= tuple[2])][['EAS BaseID','Address']]
    else:
        found=addresses[(addresses['Street Name']==tuple[4]) & (addresses['Address Number']>=tuple[0]) &(addresses['Address Number']<=tuple[2])][['EAS BaseID','Address']]

    # print(tuple,found[['Address','Street Type']].values)

    return found.values

def main(limit_rows=None):
    addresses=pd.read_csv('./raw_csv/Addresses_-_Enterprise_Addressing_System.csv')

    taxes=pd.read_csv('./Historic_Secured_Property_Tax_Rolls.csv',nrows=limit_rows)
    #print(taxes[taxes['Block and Lot Number']=='8720311'])
    # print(taxes.groupby('Block and Lot Number')['Block and Lot Number'].count().__len__())

    taxes=pd.DataFrame(taxes.groupby(['Block and Lot Number','Property Location'])['Closed Roll Fiscal Year'].count()).reset_index()
    taxes.drop('Closed Roll Fiscal Year',axis=1,inplace=True)

    taxes['parsed']=taxes['Property Location'].apply(parse_address)

    # print(taxes['parsed'])


    taxes['addresses_found']=taxes['parsed'].apply(lambda x:find(x,addresses))

    tax_map={'Tax_ID':[],'Tax_ID_Location':[],'EAS BaseID Matched':[],'EAS_Address_Found':[]}
    for i in taxes.itertuples(index=False):

        for j in i[3]:
            tax_map['Tax_ID'].append(i[0])
            tax_map['Tax_ID_Location'].append(i[1])
            tax_map['EAS BaseID Matched'].append(j[0])
            tax_map['EAS_Address_Found'].append(j[1])

    tax_map=pd.DataFrame.from_dict(tax_map)
    tax_map.index=tax_map['Tax_ID']
    tax_map.drop('Tax_ID',axis=1,inplace=True)

    print(tax_map)
    tax_map.to_csv('./Processed/tax_map.csv')


if __name__=='__main__':
    main()