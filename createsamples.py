import pandas as pd
from random import randrange, sample
import asyncio
import time
from tqdm import tqdm
full_df = pd.DataFrame()
df = pd.read_csv("processedTMSC2014.tsv",sep='\t', engine="python")
df.drop(["Unnamed: 0"],axis=1, inplace=True)
unique_users =  pd.unique(df["User ID (anonymized)"])
unique_checkins = df[['Venue ID (Foursquare)','Venue category ID (Foursquare)', 'Venue category name (Fousquare)','Latitude', 'Longitude', 'UTC time','Time']]
#unique_checkins
numberOfCheckins = len(unique_checkins)
number_of_users = 20000
number_of_threads = 1000
users_per_thread = int(number_of_users / number_of_threads)
def add_checkins(userStartId):
    global unique_checkins
    new_dataframe = pd.DataFrame()
    for userid in range(userStartId, userStartId+users_per_thread):
        #How many checkin do we want for this user?
        user_checkin_number = randrange(800,1200)
        #Generating checkin for the user
        t0 = time.clock()
        #Sampling the checkin from the checkins that we have originally
        sampled_checkin = unique_checkins.iloc[sample(range(numberOfCheckins), user_checkin_number),:]
        #Making a row to append later to dataframe
        new_row = pd.DataFrame({'User ID (anonymized)':userid,
        'Venue ID (Foursquare)':sampled_checkin['Venue ID (Foursquare)'],
        'Venue category ID (Foursquare)':sampled_checkin['Venue category name (Fousquare)'], 
        'Venue category name (Fousquare)':sampled_checkin['Latitude'],
        'Latitude':sampled_checkin['Latitude'], 
        'Longitude':sampled_checkin['Longitude'], 
        'UTC time':sampled_checkin['UTC time'],
        'Time':sampled_checkin['Time']})
        new_dataframe = new_dataframe.append(new_row, ignore_index=True)
        t1 = time.clock()
        #print(f"Data added for user number: {userid} and numbered: {user_checkin_number} and time {t1-t0} seconds.")
    return new_dataframe

t0 = time.clock()

async def run(tasks, *args):
    loop = asyncio.get_event_loop()
    futures = [
        loop.run_in_executor(
            None,
            add_checkins,
            task
        )
        for task in tasks
    ]
    results = [await f for f in tqdm(asyncio.as_completed(futures), total=len(tasks))]
    print("done with loop")
    return results

threads = [users_per_thread*i+1 for i in range(number_of_threads)]
loop = asyncio.get_event_loop()
results = loop.run_until_complete(run(threads))
full_df = pd.concat(results)
print(f"Time taken: {time.clock() - t0} seconds")
new_dataframe = full_df.sort_values(by=['Time'], ascending=True)
new_dataframe.to_csv("newDataSet.tsv", sep="\t")