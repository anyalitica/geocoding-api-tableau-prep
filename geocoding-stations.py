#import the libraries needed to access the API (requests), 
# work with JSON files (json), and Pandas data frames (pandas)

import requests
import json
import pandas as pd

#set the base URL and the endpoint
api_url_base = 'http://api.postcodes.io'
postcode_lookup_endpoint = 'postcodes/'

# build and format endpoint URL
postcode_lookup_url = '{0}/{1}'.format(api_url_base, postcode_lookup_endpoint)

# define request headers
headers = {'Content-Type': 'application/json'}

# lookup and return geocodes for an array of postcodes (up to 50 per request).
# args: [str] postcodes
# returns: [dict] [{"postcode": postcode, "longitude": longitude, "latitude": latitude}]

def bulk_geocode_by_postcode(df):
    # bring in the column with postcodes from the Input dataset
    # "station_postcode" is the name of the field in my data set that holds postcodes

    postcodes = df["station_postcode"].tolist()
    
    print('Original list of postcodes:')
    print(postcodes)
    print('=-'*40)

    # build request body, add array of postcodes
    body = json.dumps({"postcodes": postcodes})

    print('Request with the list of postcodes:')
    print(body)
    print('=-'*40)

    # make an API call using endpoint URL for postcodes lookup. 
    # Provides headers and request body
    
    response = requests.post(postcode_lookup_url,headers=headers, data=body)
    
    # If successful request then parse response and extract longitude and latitude each postcode from response
    # else return None

    if response.status_code == 200:
        parsed_response = json.loads(response.content.decode('utf-8'))
        results = parsed_response['result']
        
        print('Response from the API:')
        print(results)
        print('=-'*40)
        
        geocodes = []
        for result in results:
            query = result['query']
            longitude = result['result']['longitude']
            latitude = result['result']['latitude']
            geocodes.append({'postcode': query, 'longitude': longitude, 'latitude': latitude})
        
        print('Formatted list of postcodes and lat/lon values:')
        print(geocodes)
        print('=-'*40)
        
        df = pd.DataFrame(geocodes)
        
        print('Dataframe with the response:')
        print(df)
        print('=-'*40)

        return df
    else:
        return None

# define the columns and their data types that are brought into Tableau Prep

def get_output_schema():
    return pd.DataFrame({
        'postcode' : prep_string(),
        'longitude' : prep_decimal(),
        'latitude' : prep_decimal()
    })
