from typing_extensions import final
from numpy.lib.arraysetops import unique
import pandas as pd
import os
from pandas.io.parsers import read_csv
from pytz import timezone
from datetime import datetime
import datetime
from rich import print, pretty
from rich.progress import track
import numpy as np
import glob 
import shutil
from pandas.core.frame import DataFrame
import webbrowser
from colorama import Fore, Style
pretty.install()



#---- Get Input from user
os.system('cls')
Subfolder_1 = "Subfolder_Stop_sequence"
Subfolder_2  = "Separated_Trip_Files"
Subfolder_3  = "Separated_Sheets"
Subfolder_4  = "combined_all_days"
Foldername = "Final_folder"

#time_now = datetime.datetime.now().strftime("%H:%M%p on %D %M, %Y")

Static_path = input('\33[92m'+"Enter Static data folder path:  ")
Real_path = input("Enter Real-time data folder path:  ")
Filepath = input("Where you want to save all Files:  ")
#extract_Filepath = input("Where you want to save all extract Files:  ")
rid = input("enter Route_Id:  ")
dd = input("enter Date of the data (DD-MM-YYYY):  ")
yn = input("Do you want to filter stop_time static data (y/n):  ")
lyn = input("Do you want to Generate link (y/n):  ")
Filename = "Final_filtered_Sheet_"+dd 
Filename_1 = "Final_prepared_Sheet_"+dd 
Filename_2 = "Final_extracted_Sheet_"+dd

#---- Create required folders 
os.system('cls')
print("Creating Required Folders....")
print('Loading...\n')

path1 = Filepath+'\\'+Subfolder_1
path2 = Filepath+'\\'+Subfolder_2
path3 = Filepath+'\\'+Subfolder_3

path4 = Filepath+'\\'+Foldername

path_static = Filepath+'\\Static'
path_real = Filepath+'\\Real Feed'
path_filter = path_real+'\\Filtered_data'
path_filter_pre = path_real+'\\Filtered_prepared_data'
path_extract = path_real+'\\Extracted_data'


isExist = os.path.exists(path_static)
if not isExist:
    os.makedirs(path_static)

isExist2 = os.path.exists(path_real)
if not isExist2:
    os.makedirs(path_real)

isExist3 = os.path.exists(path_filter)
if not isExist3:
    os.makedirs(path_filter)

isExist4 = os.path.exists(path_extract)
if not isExist4:
    os.makedirs(path_extract)
    
isExist5 = os.path.exists(path_filter_pre)
if not isExist5:
    os.makedirs(path_filter_pre)

path7 = path_real+'\\'+Subfolder_4
path5 = path_filter+'\\'+Filename+'.csv'
path8 = path_filter_pre+'\\'+Filename_1+'.csv'
path6 = path_extract+'\\'+Filename_2+'.csv'

if os.path.exists(path1):
    shutil.rmtree(path1)
if os.path.exists(path2):
    shutil.rmtree(path2)
if os.path.exists(path3):
    shutil.rmtree(path3)
if os.path.exists(path4):
    shutil.rmtree(path4)
if os.path.exists(path7):
    shutil.rmtree(path7)
if os.path.exists(path5):
    os.remove(path5)
if os.path.exists(path6):
    os.remove(path6)
if os.path.exists(path8):
    os.remove(path8)
os.makedirs(path1)
os.makedirs(path2)
os.makedirs(path3)
os.makedirs(path7)

#os.makedirs(path4)

#os.makedirs(Filepath+'\\'+Foldername)

#---- Create Empty data frames
final_link_sheet = pd.DataFrame()
stage1 = pd.DataFrame()
Final_all = pd.DataFrame()
finalexcelsheet = pd.DataFrame()
finalexcelsheet_2 = pd.DataFrame()
Stop_time = pd.DataFrame()

#---- Define dependenties
filenames = glob.glob(Real_path + "\*.csv")

#---- Get Static data in CSV format
trip =  pd.read_csv(Static_path+ '\\trips.txt', sep=',')
shape = pd.read_csv(Static_path+  '\\shapes.txt', sep=',')
stime = pd.read_csv(Static_path+  '\\stop_times.txt', sep=',')

#=========================================== Create Link file =======================================

#---- Separate shape id with given route Id
trip.route_id = trip.route_id.astype(str)
trip['route'] = trip.route_id.str[:-5]
newdf = trip[(trip.route == rid)]
Unique_shape = newdf.shape_id.unique()
Unique_trip = newdf.trip_id.unique()
print(Unique_trip)
#---- Separate tripid in stop_time
if yn == 'y':
    os.system('cls')
    for tripid in track(Unique_trip, description='[yellow]Preparing Stop_time file'):
        stime2 = stime[(stime.trip_id == tripid)]
        # copy stop_id
        stopy = stime2[['stop_id']]
        stime2.rename(columns={'stop_id': 'stop_id_x'},inplace=True)
        stime2['stop_id_y'] = stopy
        stime2['stop_id_y'] = stime2['stop_id_y'].shift(-1).fillna('')

        # copy sequence_id
        stopy = stime2[['stop_sequence']]
        stime2.rename(columns={'stop_sequence': 'stop_sequence_x'},inplace=True)
        stime2['stop_sequence_y'] = stopy
        stime2['stop_sequence_y'] = stime2['stop_sequence_y'].shift(-1).fillna('')

        
        # copy arrival time
        stopy = stime2[['arrival_time']]
        stime2.rename(columns={'arrival_time': 'arrival_time_x'},inplace=True)
        stime2['arrival_time_y'] = stopy
        stime2['arrival_time_y'] = stime2['arrival_time_y'].shift(-1).fillna('')
    

        # copy departure_time
        stopy = stime2[['departure_time']]
        stime2.rename(columns={'departure_time': 'departure_time_x'},inplace=True)
        stime2['departure_time_y'] = stopy
        stime2['departure_time_y'] = stime2['departure_time_y'].shift(-1).fillna('')
        

        #Drop the empty cell rows
        stime3 = stime2[(stime2.stop_id_y != '')]
        Stop_time = Stop_time.append(stime3, ignore_index=True)

    Stop_time.to_csv(path_static+  '\\stop_times_'+rid+'.csv', index= False)

#---- Start process for each shape Id

os.system('cls')
for shapeid in Unique_shape:
    #---- Generate link    
    i=int(shapeid)
    newdf = shape[(shape.shape_id == i )]
    newdf.shape_pt_sequence=newdf.shape_pt_sequence.astype(str)
    newdf['link'] = newdf.shape_id.astype(str) + '_'+ newdf['shape_pt_sequence'].str[:-4]

    # copy shape_pt_lat
    stopy = shape[['shape_pt_lat']]
    newdf.rename(columns={'shape_pt_lat': 'shape_pt_lat_x'},inplace=True)
    newdf['shape_pt_lat_y'] = stopy
    newdf['shape_pt_lat_y'] = newdf['shape_pt_lat_y'].shift(-1).fillna('')

    # copy shape_pt_lon
    stopy = shape[['shape_pt_lon']]
    newdf.rename(columns={'shape_pt_lon': 'shape_pt_lon_x'},inplace=True)
    newdf['shape_pt_lon_y'] = stopy
    newdf['shape_pt_lon_y'] = newdf['shape_pt_lon_y'].shift(-1).fillna('')

    newdf2 = newdf[(newdf.shape_pt_lat_y != '')]
    final_link_sheet = final_link_sheet.append(newdf2, ignore_index=True)
    #print(newdf['link'])
    #============================================ Separate shape Id from raw data ====================
    os.system('cls')
    print("\n*** separate shape id: "+shapeid+" from the raw raw data\n")
    for file in track(filenames, description='[green]Scraping data'):
        df = pd.read_csv(file)
        rawdf = df[(df.shape_id == shapeid )]
        stage1 = stage1.append(rawdf, ignore_index=True)



stage1.to_csv(Filepath+'\\Sheet_Stage_1.csv',index=False)
if lyn == 'y':
    final_link_sheet.to_csv(path_static +'\\Link_'+rid+'.csv', index=False)
stagex = pd.read_csv(Filepath+'\\Sheet_Stage_1.csv')
#===================================== Convert timestamp ==========================

#df = pd.read_csv("D:\GTFS Filter\Tset 1\\Route_444_TU.csv", usecols=range(0,16))

os.system('cls')
print("\n*** Coverting EPOCH Timestamp to datetime .......\n")
print('Loading...\n')
stagex['arrival_time'] = pd.to_datetime(stagex['arrival_time'],unit='s')
stagex['departure_time'] = pd.to_datetime(stagex['departure_time'],unit='s')
stagex['timestamp'] = pd.to_datetime(stagex['timestamp'],unit='s')
fmt = "%Y-%m-%d %H:%M:%S"
stagex.arrival_time = pd.DatetimeIndex(stagex.arrival_time).tz_localize('GMT').tz_convert('Australia/Brisbane').strftime(fmt)   
stagex.departure_time = pd.DatetimeIndex(stagex.departure_time).tz_localize('GMT').tz_convert('Australia/Brisbane').strftime(fmt)
stagex.timestamp = pd.DatetimeIndex(stagex.timestamp).tz_localize('GMT').tz_convert('Australia/Brisbane').strftime(fmt)
stagex.to_csv(Filepath +'\\Sheet_Stage_2.csv', index=False)
df = pd.read_csv(Filepath +'\\Sheet_Stage_2.csv')

#==================================== Group data by trip ID========================

#.................First group by Trip ID...................
os.system('cls')


for group, file_df in track(df.groupby('trip_id'), description='[red]Filter Error in the file\n'):
    finals = pd.DataFrame()
    os.makedirs(Filepath+'\\'+Subfolder_2+'\\'+f'{group}')
    file_df.to_csv(Filepath+'\\'+Subfolder_1+'\\'+f'{group}.csv', index = False)
    df1 = pd.read_csv(Filepath+'\\'+Subfolder_1+'\\'+f'{group}.csv')

    # While filter by Trip_id for each trip group stop_sequence

    for group1, seq in df1.groupby('stop_sequence'):
        seq.to_csv(Filepath+'\\'+Subfolder_2+'\\'+f'{group}\\'+f'{group1}.csv', index = False)
    filenames = glob.glob(Filepath+'\\'+Subfolder_2+'\\'+f'{group}' + "\*.csv")
    for file in filenames:
        df_all = pd.read_csv(file)

        df_all['arrival_time'] = pd.to_datetime(df_all['arrival_time'])
        df_all['departure_time'] = pd.to_datetime(df_all['departure_time'])
        sample = df_all.departure_time - df_all.arrival_time
        df_all['sample'] = sample / np.timedelta64(1,'s')
        df_all['sample'] = df_all['sample'].fillna(0)
        df_all['sample'] = df_all['sample'].astype('int')
        newdf= df_all[(df_all['sample'] < 600)]
        timemax = newdf['sample'].max()
        newdf2 = newdf[(newdf['sample'] == timemax)]
        finals = finals.append(newdf2, ignore_index=True)

    finalexcelsheet1=finals.sort_values(by ='stop_sequence')
    finalexcelsheet1.drop_duplicates(subset ="stop_sequence",keep = "last" , inplace = True)
    finalexcelsheet1.to_csv(Filepath+'\\' +Subfolder_3+'\\'f'{group}.csv',index=False) 

#---- Append stop sequence  after filter
os.system('cls')
filepath2 = glob.glob(Filepath+'\\'+Subfolder_3+ "\*.csv")
for file in track(filepath2, description='[green] Reorganising filtered file\n\n'):
    df_all1 = pd.read_csv(file)
    finalexcelsheet = finalexcelsheet.append(df_all1, ignore_index=True)
    file_df = df_all1[['trip_id','route_id','start_time', 'start_date','stop_id','stop_sequence','timestamp','shape_id', 'arrival_time', 'departure_time','sample']]
    file_df.stop_sequence = file_df.stop_sequence.astype(int)
    file_df['link'] = file_df.shape_id.astype(str) + '_'+ file_df.stop_sequence.astype(str)
    # copy stop_id
    stopy = df_all1[['stop_id']]
    file_df.rename(columns={'stop_id': 'stop_id_x'},inplace=True)
    file_df['stop_id_y'] = stopy
    file_df['stop_id_y'] = file_df['stop_id_y'].shift(-1).fillna('')

    # copy sequence_id
    stopy = df_all1[['stop_sequence']]
    file_df.rename(columns={'stop_sequence': 'stop_sequence_x'},inplace=True)
    file_df['stop_sequence_y'] = stopy
    file_df['stop_sequence_y'] = file_df['stop_sequence_y'].shift(-1).fillna('')

    
     # copy arrival time
    stopy = df_all1[['arrival_time']]
    file_df.rename(columns={'arrival_time': 'arrival_time_x'},inplace=True)
    file_df['arrival_time_y'] = stopy
    file_df['arrival_time_y'] = file_df['arrival_time_y'].shift(-1).fillna('')
    file_df['arrival_time_y'] = pd.to_datetime(file_df['arrival_time_y'])

     # copy departure_time
    stopy = df_all1[['departure_time']]
    file_df.rename(columns={'departure_time': 'departure_time_x'},inplace=True)
    file_df['departure_time_y'] = stopy
    file_df['departure_time_y'] = file_df['departure_time_y'].shift(-1).fillna('')
    file_df['departure_time_x'] = pd.to_datetime(file_df['departure_time_x'])

    #link travel time
    #Travel_time = file_df['Actual_departure_x']-file_df['Actual_arrival_y']
    #Travel_time = pd.to_datetime(travel_time,unit='s')

    """file_df['Travel_Time'] = file_df['arrival_time_y'] - file_df['departure_time_x']
    file_df['Travel_Time'] = file_df['Travel_Time'] / np.timedelta64(1,'m')"""
    #file_df['Travel_Time'] = pd.to_datetime(df['Travel_Time'], unit='m')
    
    #Drop the empty cell rows
    file_df2 = file_df[(file_df.stop_id_y != '')]
    finalexcelsheet_2 = finalexcelsheet_2.append(file_df2, ignore_index=True)
    
finalsheet = finalexcelsheet[['trip_id','start_time', 'start_date','route_id','stop_id','stop_sequence','timestamp','shape_id', 'arrival_time', 'departure_time']]
finalsheet.to_csv(path_filter+'\\'+Filename+'.csv',index=False)
finalexcelsheet_2.to_csv(path_filter_pre+'\\'+Filename_1+'.csv',index=False)

shutil.rmtree(Filepath+'\\'+Subfolder_1)
shutil.rmtree(Filepath+'\\'+Subfolder_2)
shutil.rmtree(Filepath+'\\'+Subfolder_3)
os.remove(Filepath +'\\Sheet_Stage_2.csv')
os.remove(Filepath +'\\Sheet_Stage_1.csv') 






def convert(seconds):
    seconds = seconds % (24 * 3600)
    hour = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
      
    return "%d:%02d:%02d" % (hour, minutes, seconds)

















# combine and find average

os.system('cls')
filenames2 = glob.glob(path_filter + "\*.csv")
combine_sheet = pd.DataFrame()
print('\n')
for file in track(filenames2, description='[green]Combining filtered all day of files'):
    df = pd.read_csv(file)
    combine_sheet = combine_sheet.append(df, ignore_index=True)
combine_sheet.to_csv(path7+'\\combined.csv', index=False)
 
#---- find average

extractsheet = pd.DataFrame()
extractsheet2 = pd.DataFrame()
df_ex = pd.read_csv(path7+'\\combined.csv')
#df_extr = pd.read_csv('D:\GTFS Filter\Final data\Combined file\\Combined_travel_time.csv')
print('\n')
for group, file_df in track(df_ex.groupby('trip_id'), description='[yellow] Extracting information \n'):
    #os.makedirs('D:\GTFS Filter\Final data\Combined file\\'+Subfolder_1+'\\'+f'{group}')
    #file_df.to_csv('D:\GTFS Filter\Final data\Combined file\\'+Subfolder_1+'\\'+f'{group}.csv',index=False)
    temp = pd.DataFrame()
    for group1, stop in file_df.groupby('stop_id'):
        #print(stop)
        
        stop['arrival_time'] = stop['arrival_time'].fillna('00:00:00')
        stop['arrival_time'] = pd.to_datetime(stop['arrival_time'])
        stop['departure_time'] = pd.to_datetime(stop['departure_time'])
        stop['arrival_time'] = stop['arrival_time'].dt.strftime('%H:%M:%S')
        stop['departure_time'] = stop['departure_time'].dt.strftime('%H:%M:%S')

        second_a = pd.to_timedelta(stop['arrival_time'],errors='coerce').dt.total_seconds()
        second_d = pd.to_timedelta(stop['departure_time'],errors='coerce').dt.total_seconds()
        
        mean_arr = second_a.mean()
        mean_dep = second_d.mean()
        
        mean_arr = pd.to_datetime(mean_arr,unit='s')
        mean_dep = pd.to_datetime(mean_dep,unit='s')
      
        stop.drop_duplicates(subset ="stop_id",keep = "last" , inplace = True)
        stop["average_arrival_time"] = mean_arr
        stop["average_departure_time"] = mean_dep
        stop['average_arrival_time'] = stop['average_arrival_time'].dt.strftime('%H:%M:%S')
        stop['average_departure_time'] = stop['average_departure_time'].dt.strftime('%H:%M:%S')
        
        stop2 = stop[['trip_id','shape_id','stop_id','start_time','stop_sequence','average_arrival_time','average_departure_time']]

        temp = temp.append(stop2, ignore_index=True)

    temp2 = temp.sort_values(by='stop_sequence')
    temp2.stop_sequence = temp2.stop_sequence.astype(int)
    temp2['link'] = temp2.shape_id.astype(str) + '_'+ temp2.stop_sequence.astype(str)
    # copy stop_id
    stop_rename = temp2[['stop_id']]
    temp2.rename(columns={'stop_id': 'stop_id_x'},inplace=True)
    temp2['stop_id_y'] = stop_rename
    temp2['stop_id_y'] = temp2['stop_id_y'].shift(-1).fillna('')

        # copy sequence_id
    stopy = temp2[['stop_sequence']]
    temp2.rename(columns={'stop_sequence': 'stop_sequence_x'},inplace=True)
    temp2['stop_sequence_y'] = stopy
    temp2['stop_sequence_y'] = temp2['stop_sequence_y'].shift(-1).fillna('')
    
         # copy arrival time
    stopy = temp2[['average_arrival_time']]
    temp2.rename(columns={'average_arrival_time': 'average_arrival_time_x'},inplace=True)
    temp2['average_arrival_time_y'] = stopy
    temp2['average_arrival_time_y'] = temp2['average_arrival_time_y'].shift(-1).fillna('')
    

     # copy departure_time
    stopy = temp2[['average_departure_time']]
    temp2.rename(columns={'average_departure_time': 'average_departure_time_x'},inplace=True)
    temp2['average_departure_time_y'] = stopy
    temp2['average_departure_time_y'] = temp2['average_departure_time_y'].shift(-1).fillna('')

    #print(temp2.average_departure_time_x)
    arrival_y = pd.to_timedelta(temp2['average_arrival_time_y'],errors='coerce').dt.total_seconds()
    departure_x = pd.to_timedelta(temp2['average_departure_time_x'],errors='coerce').dt.total_seconds()
    #print(arrival_y)
    #print(departure_x)
    travel_time = arrival_y-departure_x
    travel_time = travel_time.fillna(0)
    #print(travel_time)
    #travel_time = travel_time[travel_time<0] = 0
    #travel_time = str(datetime.timedelta(seconds = travel_time))
    
    
    #print(travel_time)
    
    temp2['travel_time'] = travel_time
    temp2['travel_time'][temp2['travel_time'] < 0] = 0
    temp2['travel_time'] = pd.to_datetime(temp2['travel_time'],unit='s')
    temp2['travel_time'] =  temp2['travel_time'].dt.strftime('%H:%M:%S')
    
    temp3 = temp2[(temp2.stop_id_y != '')]
    extractsheet = extractsheet.append(temp3, ignore_index=True)
    
#print(temp2)

extractsheet.to_csv(path6, index=False)





#---- Remove folder after All work

#shutil.rmtree(Filepath+'\\'+Subfolder_4)


#os.system('cls')





print("\n Your file is ready to Visualize [yellow](*_*)\n")
webbrowser.open(Filepath)
