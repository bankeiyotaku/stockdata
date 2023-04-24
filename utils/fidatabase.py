import os
import mysql.connector as mysql
import pandas as pd
from dotenv import load_dotenv

import refinitiv.data as rd
from refinitiv.data.content import symbol_conversion

from queue import Queue
from threading import Thread
import pandas_market_calendars as mcal

import numpy as np
from datetime import datetime

import time
import math


from . import variant_callback as vc





def ConnectToDb():
    load_dotenv()

    #HOST = "raster.mysql.database.azure.com" # or "domain.com"
    HOST = os.getenv("MYSQL_HOST")
    # database name, if you want just to connect to MySQL server, leave it empty
    DATABASE = os.getenv("MYSQL_DATABASE")
    # this is the user you create
    #USER = "django"
    USER=os.getenv("MYSQL_USER")

    # user password
    #PASSWORD = "Sylvina123!"
    PASSWORD=os.getenv("MYSQL_PASSWORD")
    # connect to MySQL server
    db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD)
    #print("Connected to:", db_connection.get_server_info())

    return db_connection



def CreateNewSeriesTable( db, rics, series):

    cdb = db.cursor()

    cdb.execute("DROP TABLE IF EXISTS `fidata`.`ts_%s`;" % series)

    sql =  "CREATE TABLE `fidata`.`ts_%s` (" % series
    sql += "`ddate` DATE NOT NULL,"
    for ric in rics:
        col = ric.replace(".","_")  
        sql += "`%s` FLOAT NULL," % col
    sql += "PRIMARY KEY (`ddate`));"

    cdb.execute(sql)

    db.commit()
    
    cdb.close()

    output = f"ts_{series}"

    #print("THIS IS IT2: ",output)

    return output


def GetSeriesMetadata( db, series_name ):

    try:
        metadata_df = pd.read_sql("SELECT series_name, rdp_series_name, rdp_series_param, base_series, update_function, start_date, end_date, ric_list FROM TimeSeries_MetaData WHERE series_name = '%s'" % (series_name), db)

        if( len(metadata_df) == 0 ):
            return( {} )
        
        out_dict = metadata_df.transpose()[0].to_dict()
        out_dict["ric_list"] = out_dict["ric_list"].split(",")

        return out_dict
    except IndexError:
        return( {} )


def UpdateSeriesMetadata ( db, series_name, mdf, write_new = False ):
   
    mdf_old = GetSeriesMetadata( db, series_name )
    if ((len(mdf_old) == 0) and (write_new == False)):
        return False
       
    cdb = db.cursor()

    mdf = mdf.copy()

    if( mdf["rdp_series_name"] == None ):
        mdf["rdp_series_name"] = "NULL"
    else:
        mdf["rdp_series_name"] = "'%s'" % mdf["rdp_series_name"]
    
    if( mdf["rdp_series_param"] == None ):
        mdf["rdp_series_param"] = "NULL"
    else:
        mdf["rdp_series_param"] = "'%s'" % mdf["rdp_series_param"]

    if( mdf["base_series"] == None ):
        mdf["base_series"] = "NULL"
    else:
        mdf["base_series"] = "'%s'" % mdf["base_series"]

    if( mdf["update_function"] == None ):
        mdf["update_function"] = "NULL"
    else:
        mdf["update_function"] = "'%s'" % mdf["update_function"]

    mdf["ric_list"] = ",".join(mdf["ric_list"])


    if( write_new ):
        sql = "DELETE FROM TimeSeries_MetaData WHERE series_name = '%s';" % series_name
        cdb.execute(sql)
        sql = "INSERT INTO TimeSeries_MetaData (series_name, rdp_series_name, rdp_series_param, base_series, update_function, start_date, end_date, ric_list) VALUES ('%s', %s, %s, %s, %s, '%s', '%s', '%s')" % (series_name, mdf["rdp_series_name"], mdf["rdp_series_param"], mdf["base_series"], mdf["update_function"], mdf["start_date"], mdf["end_date"], mdf["ric_list"])
    else:
        sql = "UPDATE TimeSeries_MetaData SET rdp_series_name = %s, rdp_series_param = %s, base_series = %s, update_function = %s, start_date = '%s', end_date = '%s', ric_list = '%s' WHERE series_name = '%s'" % (mdf["rdp_series_name"], mdf["rdp_series_param"], mdf["base_series"], mdf["update_function"], mdf["start_date"], mdf["end_date"], mdf["ric_list"], series_name)
    #sql = "UPDATE TimeSeries_MetaData SET rdp_series_name = %s, rdp_series_param = '%s', start_date = '%s', end_date = '%s', ric_list = '%s' WHERE series_name = '%s'" % (mdf["rdp_series_name"], mdf["rdp_series_param"], mdf["start_date"], mdf["end_date"], mdf["ric_list"], series_name)
    
    print(sql)
    cdb.execute(sql)
     
    db.commit()
    cdb.close()

    return True


def ReadSeriesFromDB( db, series_name ):
    #series_name = "Price"
    metadata_df = GetSeriesMetadata( db, series_name)
    #pd.read_sql("SELECT series_name, rdp_series_name, rdp_series_param, start_date, end_date, ric_list FROM TimeSeries_MetaData WHERE series_name = '%s'" % (series_name), db)

    if(metadata_df["base_series"] == None):


        #display(metadata_df)
        db_df = pd.read_sql(f"SELECT * FROM ts_{series_name}", db)
        db_df.set_index("ddate", inplace=True)


        # Rename the columns of db_df to change the "_" to "."
        db_df.columns = [ric.replace("_", ".") for ric in db_df.columns]

        db_first_date = db_df.first_valid_index()
        db_last_date = db_df.last_valid_index()

        if( metadata_df["start_date"] != db_first_date ):
            print("Error: Start date in metadata does not match start date in database")

        if( metadata_df["end_date"] != db_last_date ):
            print("Error: End date in metadata does not match end date in database")

        ric_list = metadata_df["ric_list"]  #[0].split(",")
        col_list = list(db_df.columns)

        ric_list = ric_list.sort()
        col_list = col_list.sort()
        if(ric_list != col_list):
            print("Error: RIC list in metadata does not match RIC list in database")    


        db_df.index = pd.to_datetime(db_df.index)
        return(db_df)
    else:
        update_function = "vc." + metadata_df["update_function"] + "(base_df)"
        base_df = ReadSeriesFromDB( db, metadata_df["base_series"])
        new_df = eval(update_function)
        return(new_df)



# Write to database
def WriteSeriesToDB( db, series_metadata, new_df, new_series=False):



    if(new_series==False):
        mdf= GetSeriesMetadata( db, series_metadata["series_name"] )
        if( len(mdf) == 0) :
            print("Series does not exist in database.  Please rerun w/ new_series=True.")
            return False


    dbs = db.cursor()   
    if(new_series==True):
        dbs.execute("DELETE FROM TimeSeries_MetaData WHERE series_name = '%s';" % series_metadata["series_name"])

    series_name = series_metadata["series_name"]
    rdp_series_name = series_metadata["rdp_series_name"]
    rdp_series_params = series_metadata["rdp_series_param"]
    base_series = series_metadata["base_series"]
    update_function = series_metadata["update_function"]

    
    if( rdp_series_name == None ):
        rdp_series_name = "NULL"
    else:
        rdp_series_name = "'" + rdp_series_name + "'"

    if( rdp_series_params == None ):
        rdp_series_params = "NULL"
    else:
        rdp_series_params = "'" + rdp_series_params + "'"


    if( base_series == None ):
        base_series = "NULL"
    else:
        base_series = "'" + base_series + "'"

    if( update_function == None ):
        update_function = "NULL"
    else:
        update_function = "'" + update_function + "'"

    


    start_date = series_metadata["start_date"]
    end_date = series_metadata["end_date"]
    ric_list = ",".join(series_metadata["ric_list"])
    
    #rdp_series_name = "TR.PriceClose"
    #rdp_series_params = "Curn=USD"
    #start_date = new_df.first_valid_index().date()
    #end_date = new_df.last_valid_index().date()
    #ric_list = ",".join(rics_of_interest)

    table_name=""

    if(new_series==True):
        table_name = CreateNewSeriesTable( db, series_metadata["ric_list"], series_name )
    else:
        table_name = "ts_" + series_name




    header_sql = f"INSERT INTO {table_name}  ( ddate, "
    for ric in  new_df.columns:
        col = ric.replace(".", "_") 
        header_sql = header_sql + col + ", "

    header_sql = header_sql[:-2] 

    row_count = len(new_df.index)

    chunk_size = 90

    for i in range(0, row_count, chunk_size):
        sql = header_sql + ") VALUES \n"

        next_chunk = min( row_count, chunk_size+i)

        for j in range(i, next_chunk):
            sql = sql + "('%s', " % (new_df.index[j].date())
            for ric in  new_df.columns:
                val = new_df.at[new_df.index[j], ric]
                if( pd.isna(val) or math.isnan(val) ):
                    sql = sql + "NULL, "
                else:
                    sql = sql + f"{val:.2f}, "

            sql = sql[:-2] 
            sql = sql + "),\n"
            

        sql = sql[:-2] 
        sql = sql + ";"
        
        #print(sql)
        dbs.execute(sql)

    meta_sql=""
    if(new_series==True):
        meta_sql = f"INSERT INTO TimeSeries_MetaData ( series_name, rdp_series_name, rdp_series_param, base_series, update_function, start_date, end_date, ric_list ) VALUES "
        meta_sql = meta_sql + f"('{series_name}', {rdp_series_name}, {rdp_series_params}, {base_series}, {update_function}, '{start_date}', '{end_date}', '{ric_list}');"
    else:
        meta_sql = f"UPDATE TimeSeries_MetaData SET end_date='{end_date}' WHERE series_name='{series_name}';"
    

    dbs.execute(meta_sql)
    db.commit()
    dbs.close()
    return True
        




def ClearDupes( input_df ):
    #
    # Find all the instances of where we have duplicate dates as indexes
    #
    dupes = input_df[ input_df.index.duplicated(keep=False) ]
    dup_frames = {}

    print( f"Found {len(dupes)} duplicate dates")

    #
    # Key a dictionary by the index value mapping a date -> dataframe of all the rows with that date
    #
    for indx, value in dupes.iterrows():
        #print('index: ', indx, 'value: ', value)
        if((indx in dup_frames)==False):
            sub_df = dupes.loc[indx].copy()
            dup_frames[indx] = sub_df


    # Create a new dataframe with the same columns as the dupes dataframe. We will put the cleaned data here
    cleaned_dupes = pd.DataFrame( np.nan, columns=dupes.columns, index=dup_frames.keys() )

    # For each date with duplicates, iterate trhough the duplicate rows and pick the best item
    for indx, value in dup_frames.items():
        #Iterate through all the items in the row
        for indx2, row in value.iterrows():
            #print(row[0], row[1])
            
            for ric,val in row.items():
                if( val is None):
                    continue

                if( pd.isna(val) ):
                    continue

                if( math.isnan(val) ):
                    continue
                
                if( math.isnan( cleaned_dupes.at[indx, ric]) ):
                    cleaned_dupes.at[indx, ric] = val
                elif( cleaned_dupes.at[indx, ric] == val ):
                    continue;
                else:
                    print("Dupliate data entry for", ric, "on", indx, "with value", val, " Clean Store: ", cleaned_dupes.at[indx, ric])


    new_df = input_df[ ~input_df.index.duplicated(keep=False) ]
    new_df = pd.concat([new_df, cleaned_dupes])
    new_df.sort_index(inplace=True)
    return new_df


def UpdateSeries( rd, db, series_name ):

    mdf = GetSeriesMetadata(db,series_name)

    base_series = mdf["base_series"]
    update_function = mdf["update_function"]

    if( type(base_series) == str ):
        base_mdf = GetSeriesMetadata(db,base_series)

        if( type( base_mdf["rdp_series_name"]) == str ):    
            result = UpdateSeriesFromRDP( rd, db, base_series)
        else:
            result = UpdateSeries( rd, db, base_series)

        updated_base_mdf = GetSeriesMetadata(db,base_series)
        if( updated_base_mdf["end_date"] > mdf["end_date"]):
            print(f"Updating {series_name} end date to {updated_base_mdf['end_date']}")
            mdf["end_date"] = updated_base_mdf["end_date"]
            UpdateSeriesMetadata(db,series_name,mdf)

    # if( type(update_function) == str ):
    #     base_df = ReadSeriesFromDB( db, base_series )
    #     update_function = update_function + "(base_df)"

    #     dupes = len( base_df[ base_df.index.duplicated(keep=False)] )

    #     print(f"Updating {series_name}:{base_series} with {update_function}")

    #     new_df = eval(update_function)
    #     new_metadata = mdf.copy()

    #     old_end_date = pd.Timestamp(pd.Timestamp(mdf["end_date"]))
    #     new_metadata["end_date"] = new_df.index[-1].date()
    #     new_subframe = new_df.loc[ new_df.index > old_end_date ]
    #     WriteSeriesToDB(db,new_metadata,new_subframe,False)
    
  


def UpdateSeriesFromRDP( rd, db, series_name ):

    mdf = GetSeriesMetadata(db,series_name)

    rdp_series_name = mdf["rdp_series_name"]
    rdp_series_params = mdf["rdp_series_param"]
    db_start_date = mdf["start_date"]
    db_end_date = mdf["end_date"]
    ric_list = mdf["ric_list"]

    start_date = (db_end_date+pd.DateOffset(days=1)).date()
    end_date = pd.Timestamp.today().date()
    if( pd.Timestamp.today().hour < 17 ):
        end_date = (end_date - pd.DateOffset(days=1)).date()

    if(start_date > end_date):
        print("No new data to pull")
        return
    
    nyse_cal = mcal.get_calendar('NYSE')
    rows_requested =  len( nyse_cal.schedule(start_date=start_date, end_date=end_date).index.unique() )
    if(rows_requested < 1):
        print("No new data to pull")
        return

    print(f"Pulling {rows_requested} rows from {start_date} to {end_date}") 
    update_df = ReadTimeSeriesFromRDP(rd,mdf["rdp_series_name"],mdf["rdp_series_param"], start_date, end_date,ric_list)
    update_df = ClearDupes(update_df)

    new_end_date = update_df.last_valid_index().date()
    mdf["end_date"] = new_end_date

    result = WriteSeriesToDB(db, mdf, update_df, False)

    return result




# FindNanRuns( input_df as DataFrame)
#
# Returns tuple:
#       ( sorted_key, run_hash )
# sorted_key - 
#       sorted list of tuples 
#       [ ( ric, missing_count ), ( ric, missing_count ), ..]
# run_hash - 
#       Dictionary 
#       key - RIC
#       value - List of lists 
#       [ [ run_length, run_start_date, run_end_date ], [ run_length, run_start_date, run_end_date ], .. ]

def FindNanRuns( input_df):
    #
    #  Find all the missing data
    #

    key_hash = {}
    run_hash = {}

    new_df = input_df.applymap( lambda x: np.nan if pd.isna(x) else float(x) )

    # Iterate through all the columns in the dataframe - each column is a different RIC
    for ric in new_df.columns:
        series = new_df[ric].copy()

        # Find the first and last valid date for the RIC
        first_idx = series.first_valid_index()
        last_idx = series.last_valid_index()

        # Create a sub series of the RIC series from the first valid date to the last valid date
        sub_series = series.loc[first_idx:last_idx]

        # Count the number of NaNs in the sub series
        na_count = sub_series.isna().sum()
        
        # Create a series of only the NaNs in the sub series
        na_series = sub_series.loc[ sub_series.isna() ]
    
        in_run = False
        run_start = None
        run_length = 0

        # Loop the the dates for the stock and find all the "runs" of NaNs
        for row in sub_series.items():

            # Is the current row a NaN?
            if( math.isnan(row[1]) ):
                #If we're already in a run, increment the run length otherwise start a new run
                if(in_run):
                    run_length += 1
                    continue
                else:
                    run_length = 0
                    run_start = row[0]
                    in_run = True
            else:
                #We found a valid value. If we're in a run, add the run to the run hash
                if(in_run):
                    # If we already have a run for this RIC, add this run to the list of runs for this RIC
                    # Otherwise, add a new entry for this RIC in the run hash table
                    if(ric in run_hash.keys()):
                        #  Add this run to the list of runs for this RIC
                        ric_run_list = run_hash[ric]
                        ric_run_list.append( [run_start.date(), row[0].date(), run_length] )
                        # Sort the run list by the run length
                        ric_run_list.sort( key=lambda x: x[2], reverse=True)
                        run_hash[ric] = ric_run_list
                    else:
                        run_hash[ric] = [[run_start.date(), row[0].date(), run_length]]
                    

                    in_run = False
                    continue
                else:
                    continue
    
        key_hash[ric] = na_count



    sorted_key = sorted(key_hash.items(), reverse=True, key=lambda x:x[1])


    return ( sorted_key, run_hash )


def ReadTimeSeriesFromRDP(rd, series_name, series_param, original_start_date, end_date_final, rics_of_interest):

    #
    # Read Time Series data from refinitiv
    #
    #
    #initialize the output dataframe
    output_df = pd.DataFrame()


    NP = 8 # number of threads to use

    # Create a queue to communicate with the worker threads
    input_queue = Queue()
    results_queue = Queue()

    # Get the NYSE calendar
    nyse_cal = mcal.get_calendar('NYSE')


    # Clear any stray data from the queues
    with input_queue.mutex:
        input_queue.queue.clear()

    with results_queue.mutex:
        results_queue.queue.clear()

    #
    # Set the start and end dates

    #end_date_final = pd.Timestamp.today().date()
    #original_start_date = pd.Timestamp(year=end_date_final.year-30,month=1,day=1).date()
    rows_requested =  len( nyse_cal.schedule(start_date=original_start_date, end_date=end_date_final).index.unique() )
    
    # Max 8 thrads
    NP = max( 1, min( int((rows_requested/70)+1),8) )
  
    field_string = series_name + "(" + series_param + ")"
    # Begin Definition of Thread Class  

    class GetDataThread(Thread):
        def __init__(self, input_queue, results_queue):
            super().__init__()

            self.input_queue = input_queue
            self.results_queue = results_queue

        def run(self):

            for data in iter(self.input_queue.get, "STOP"):
                ric_list = data[0]
                start_date = data[1]
                end_date = data[2]
                vd_count = data[3]
                cd_count = data[4]
                
                print(f"Worker Queue Getting {vd_count} biz days for {start_date} to {end_date}")
                while True:
                    #time.sleep(np.random.uniform(.5, 3))
                    
                    df = rd.get_history(
                        universe=ric_list,
                        fields=[field_string],
                        interval="1D",
                        start=start_date,
                        end=end_date)
                    
                    if( len(df) == 0 ):
                        print(f"No data for {vd_count}:{start_date} to {end_date} -- retrying")
                        # Set tr to a random floating point number between 1 and 2
                        time.sleep(np.random.uniform(2.5,4))
                        continue

                    expected_dates=nyse_cal.valid_days(start_date=start_date, end_date=end_date)
                    # Select the last date in the schedule

                    first_date = df.first_valid_index().date()
                    last_date = df.last_valid_index().date()

                    expected_first_date = expected_dates[0].date()
                    expected_last_date = expected_dates[-1].date()

                    if( first_date > expected_first_date ):
                        print(f"WARNING: Expected Start: {expected_first_date} but got {first_date}")
                        #print(f"Retrying")
                        #continue

                    if( last_date < expected_last_date):
                        print(f"WARNING: Expected End {expected_last_date} but got {last_date}")
                        #print(f"Retrying")
                        #continue
                
                    if( len(df) < vd_count ):
                        print(f"WARNING: Expected rows {vd_count} but got {len(df)}")
                        #print(f"Retrying")
                        #continue

                    self.results_queue.put([df,start_date, end_date, vd_count, cd_count])
                    break

            print("STOPPING WORKER QUEUE")

    # Class Definition End

    # Main code

    t_start = time.perf_counter()

    #
    #  Break the request into chunks of 100 days
    #

    day_count = 100
    start_date = original_start_date
    end_date =  (start_date + pd.DateOffset(days=day_count)).date()


    # This is the number of days calendar days between the start and end dates
    total_days_to_get =(end_date_final - start_date).days
    # Initialie the total number of chunks to 0 -- will be incremented in the loop
    total_chunks=0

    # Set the current position to the start date
    current_pos = start_date

    while current_pos <= end_date_final:

        start_date = current_pos
        end_date = min( end_date_final, (start_date + pd.DateOffset(days=day_count)).date() )

        valid_day_count = len( nyse_cal.schedule(start_date=start_date, end_date=end_date).index.unique() )
        calendar_days = (end_date - start_date).days + 1

        total_chunks = total_chunks + 1

        #
        # Add the chunk to the input queue
        #
        input_queue.put([rics_of_interest, start_date, end_date, valid_day_count, calendar_days])
        
        current_pos = (end_date + pd.DateOffset(days=1)).date()
        continue

    #
    # Put the STOP signal on the queue so the threads kno2 to terminate
    #
    for _ in range(NP):
        input_queue.put("STOP")

    # Spin up the worker threads
    for _ in range(NP):
        GetDataThread(input_queue, results_queue).start()
        if(NP>1):
            time.sleep(np.random.uniform(.25, 1.5))

        

    #
    # Read the results
    #
    for i in range(total_chunks):
            chunk_data = results_queue.get()
            chunk_df = chunk_data[0]
            chunk_start_date = chunk_data[1]
            chunk_end_date = chunk_data[2]

            chunk_valid_day_count = chunk_data[3]
            chunk_calendar_day_count = chunk_data[4]

            chunk_number = int((chunk_end_date - original_start_date).days / day_count )
            if(chunk_calendar_day_count<day_count):
                chunk_number = chunk_number + 1

            chunk_df_width = chunk_df.shape[1]
            chunk_df_len = chunk_df.shape[0]
            chunk_df_unique_dates = len( chunk_df.index.unique() )

            output_df = pd.concat([output_df, chunk_df], axis=0)
            print(f"\nCHUNK:{chunk_number}-{total_chunks} {chunk_df_width}x{chunk_df_len} {chunk_df_unique_dates}-{chunk_valid_day_count} days ({chunk_start_date} to {chunk_end_date})")

    t_end = time.perf_counter()

    output_df.sort_index(inplace=True)
    output_df_len = len(  output_df )


    print(f"Output DF: {output_df_len} rows Requested: {rows_requested} rows")

    time_elapsed = t_end - t_start
    time_per_year = time_elapsed / (total_days_to_get / 365)
    print(f"Time elapsed: {time_elapsed:0.4f} seconds. {time_per_year:0.4f} seconds per year of data")

    output_df = output_df.applymap( lambda x: np.nan if pd.isna(x) else float(x) )

    return output_df





def getSPYList(rd, index_date):
    spy_history = rd.get_data(universe=['.SPX'],
                        fields=['TR.IndexJLConstituentRIC',
                        'TR.IndexJLConstituentName.value',
                        'TR.IndexJLConstituentRIC.change',
                        'TR.IndexJLConstituentChangeDate.value'],
                        parameters={"SDate":"1990-01-01","EDate":"2023-04-05","IC":"B"})



    spy_history.sort_values(by = ['Date','Change'], ascending=[True, True], inplace=True)
    spy_history.set_index("Date", inplace=True)
    spy_history.drop(columns="Instrument",inplace=True)

    spy_on_date = {}

    spy_subset = spy_history.loc[ (spy_history.index.date <= index_date) ]
    spy_subset.sort_index()

    for index, row in spy_subset.iterrows():
        if index > index_date:
                print("INDEX!!")
        if row["Change"] == "Joiner":
            if row["Constituent RIC"] in spy_on_date:
                print("Joined twice:", row["Constituent RIC"])
                spy_on_date[row["Constituent RIC"]] += 1
            else:
                spy_on_date[ row["Constituent RIC"] ] = 1
        elif row["Change"] == "Leaver":
            if row["Constituent RIC"] in spy_on_date:
                if( spy_on_date[row["Constituent RIC"]] > 1 ):
                    print("Deleting added ref", row["Constituent RIC"])
                    spy_on_date[row["Constituent RIC"]] -= 1
                else:
                    del spy_on_date[row["Constituent RIC"]]
            else:
                print("Not found to remove!: ", row["Constituent RIC"])
        else:
            print("Leaver/Joiner badly formed:", row["Change"])

    spy_subset = list( spy_on_date.keys() )

    return (spy_subset)




if __name__ == '__main__':
    # Execute when the module is not initialized from an import statement.

    rd.open_session()

    rics_of_interest = getSPYList(rd, pd.Timestamp.today().date())

    end_date = pd.Timestamp.today().date()
    start_date = pd.Timestamp(year=end_date.year-1,month=1,day=1).date()

    output_df = ReadTimeSeriesFromRDP(rd,"TR.PriceClose","Curn=USD", start_date, end_date, rics_of_interest)

    print(output_df.tail())


#print(rics_of_interest)


