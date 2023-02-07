# Data extraction from the open-weather API
# In this exercise, you will extract weather data about several cities and save it to a CSV file. We need the following weather information for each city:
# - *city_name.* 
# - *country.* Name of country where city is.
# - *date.* Keep only the date without time
# - *hour.* Show the hour (e.g., 16, 10, 15)
# - *temp.* Temperature
# - *temp_min.* Minimum temperature
# - *temp_max*. Maximum temperature
# - *humidity.*
# - *rain.* Get rain volume for the last 3 hours
# 
# Please use the description above as column names in your dataframe. For further explanattion of the output, see screenshot below.
# 
# Please retrieve and get data for the following cities: ```'LiLongwe', 'KIGali', 'Blantyre', 'Lusaka', 'Tokyo', 'Yaoundé', 'Zomba', 'Mzuzu', 'New York', 
# 'limbe', 'Cape town'```
# 
# Please use the API documentation to get the base-url and all required parameters for making calls to the API.

# ## Python-setup
# Import all the libraries you need 



# Import Python packages here
import requests
import json
import pandas as pd


# ## Define functions to extract data from the API




def city_id_from_name(city_name=None, city_list_json_file='city.list.json'):
    """
    Given city name, returns city_id for use in the weather API
    Arguments:
    city_name -- name of city we want city_id for 
    city_list_json_file -- full path to json filee containing city names and id's
    """
    # open json file like this: open(jsofile,encoding="utf8" )(~ 1 line)
    fopen = open(city_list_json_file,encoding="utf8" )
    
    # use json.load() load the json file opened above(~ 1 line)
    # Note that the resulting object is a list
    city_list = json.load(fopen)
    
    # Write a for loop to go through the list above and do the following
    # 1. cheeck if city_name is equal to our target city name, if so
    # save it in a variable, break out of the loop 
    # and return the city_id
    # Remember that the contents of the list are dictionaries
    # Make sure you convert both strings to lower case before comparing them
    # (~ 3 lines) 
    Id_city=None
    for lis in city_list:
        if lis['name'].lower()==city_name.lower():
            Id_city=lis['id']
            break          
    # Remember to return the city_id
    return Id_city


# In[101]:


def get_weather(base_url, api_key=None, city_id=None, city_name=None):
    """
    Returns current weather for this city as a dataframe
    :param base_url --  the base API url
    :param api_key -- your API key
    :param city_name: city name
    :param city_id: city id
    :return:
    """
    
    # add your API key
    url = "{}{}&APPID={}".format(base_url, city_id, api_key)

    # use requests to retrieve data from the API
    # ~ 1 line
    response=requests.get(url)
    # retrieve JSON from the response object above
    # ~ 1 line
    json_object = response.json()
    # inspect the json object and decide how to get the data that we need
    # using dictionary style indexing and load the results into a list
    # ~ 1 line
    data=json_object['list']
    # We also need city information such as country, use similar 
    # dictionary indexing to retrieve the info and put into variable 
    # ~ 1 line
    data_city=json_object['city']
    # Create a list to hold the data items, which will be daily
    # weatheer forecasts 
    # ~ 1 line
    weatheer_for=[]
    #weatheer_for=[city_name, country, date, hour, temp, temp_min, temps_max, humidity, rain]
    # Loop through the data_items and retrieve the data we need
    # Do the foowing in the loop
    # 1. Create a dictionary and add 'country' and 'city_name' using city_details
    # 2. Add the rest of the items: temp, temp_max etc to the dictionary
    # 3. add this particullar data item to the data list
    # in the loop, use a temporary variable to carry text date info
    # so that you can convert it to Python date later
    # note that in some items, rain informaion is not available
    # find a solution to deal with this issue
    # ~ 10-11 lines
    for weather in data:
        dic={}
        dic['name']=data_city['name']
        dic['country']=data_city['country']
        dic['date']= weather['dt_txt'].split()[0]
        dic['hour']=','.join(weather['dt_txt'].split()[1].split(':'))
        dic['temp']= weather['main']['temp']
        dic['temp_min']=weather['main']['temp_min']
        dic['temp_max']=weather['main']['temp_max']
        dic['humidity']=weather['main']['humidity']
        if 'rain' in weather.keys():
            dic['rain']=weather['rain']['3h']
        weatheer_for.append(dic)
    # create a dataframe from the data list
    # ~ 1 line
    data_weath=pd.DataFrame(weatheer_for)
    # add time aware datetime using function  pd.to_datetime() 
    # with input in the function being the date_txt column
    # ~ 1 line
    data_weath['date']=pd.to_datetime(data_weath['date'])
    
    # add date and hour using avaiable functionality on date objects
    # For example, to get date from datetime, do datetime.date()
    # use df.apply(lambda x:) type of syntax for this
    # ~ 2 lines
    
    
    # drop columns we dont need, we only need columns specified 
    # in the problem description
    
    
    # Remember to return the dataframe
    return data_weath


# ## Putting it all together
# Define a helper functions to go through a list of countries and retrieve data by calling the functions above.

# In[102]:


def retrieve_and_save_weather_data(city_list, output_csv):
    """
    Helper function putting everything together.
    Arguments:
    url -- list of cityt names to get data for
    output_csv -- full path to CSV where to sav data
    """
    
    # base weather API url
    # ~ 1 line
    base_url = "http://api.openweathermap.org/data/2.5/forecast?id="
    # REPLACE WITH YOUR OWN OR YOU CAN USE MINE
    # in case your key isnt working, use mine: 'cd689df7ce5a01db2aafde528e3d87c4'
    # ~ 1 line
    API_KEY = '8a479d4cf2e3f9a768ee2f89486afc80'
    # list to hold dataframes for each city
    # ~ 1 line
    data_list=[]
    # Loop through list of city names and do the following
    for names in city_list:
        # 1. Get city id
        ID_city=city_id_from_name(names)
        # 2. get weather data for this city
        data=get_weather(base_url, API_KEY, ID_city, names)
        # 3. Add this weather data into the list above
        data_list.append(data)
    # ~ 3 lines
    # use pandas function pd.concat() to combine dataframes in the list into a single
    Final=pd.concat(data_list)
    # dataframe and save to CSV file without index
    Final.to_csv(output_csv, index=False)


# In[103]:


# call function here with the city names provided in the introduction.
# Make sure to also pass full-path for output CSV file
#, 'KIGali', 'Blantyre', 'Lusaka', 'Tokyo', 'Yaoundé', 'Zomba', 'Mzuzu', 'New York', 'limbe', 'Cape town'
city_names = ['LiLongwe','KIGali', 'Blantyre', 'Lusaka',  'Yaounde', 'Tokyo','Zomba', 'Mzuzu','New York', 'limbe', 'Cape town']
output_file = 'Weather_data.csv'

# Call the function to extract and save data here
retrieve_and_save_weather_data(city_names, output_file)


# In[104]:


pd.read_csv('Weather_data.csv')


# ## Submission and grading notes
# 1. After you are done testing in this notebook, please submit your solution as a ```Python (.py)``` file
# 2. Marks will be provided based on whether your Python script runs without errors, saves data and accuracy of the data saved (e.g., contains all required columns, has all the expected number of rows)
