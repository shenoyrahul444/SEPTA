import pandas as pd
import urllib.request, json
from DB_Connection  import DB_Connection      # Imported from another local module to handle database interactions
import time
from sqlite3 import Error
import numpy as np

# STEP 1 - Task 1

def fetchDataFromURL(url):

    """
    Collect data from API source, reading it as JSON and sending it back
    # *Note- We could have just separated the data and metadata here, However, since the Problem statement says otherwise, I have not proceeded in that direction
    """
    if url == "" or url == None:
        return -1

    with urllib.request.urlopen(url) as url:
        # data = json.loads(url.read().decode())
        data = json.loads(url.read())
        return data

# STEP 1 - Task 2
def convertToRelationalFormat(json_obj,N ,headers):
    """
        1> Convert JSON object into a relational table(Dataframe)
        2> Truncate the top N lines
        3> Apply headers passed to the relational table structure(Dataframe)
    """
    #Edge Cases
    if json_obj == None or N < 0 or headers == [] or headers == None:
        return -1

    if N > 0:
        df = pd.DataFrame(json_obj)     # Converting JSON Data into a relational form(Dataframe)
        df = df.drop(['metadata'],axis=1)
        truncated_df = df.iloc[N:]      # Truncating the top 'N' lines
        truncated_df = truncated_df.reset_index()
        truncated_df.columns = headers      # Applying the headers to the Dataframe
        return truncated_df
    elif N == 0:

        # Problems we encountered during transforming latest data into dataframe. So have separated the 'data' from 'metadata'
        # and enabled the relational transformation
        df = pd.DataFrame(json_obj['data'],columns=headers)
        return df


# STEP 1 - Task 3
def uploadDataFromDataFrameToDatabase(credentials,table_name,data):
    """
    Given some database credentials, a table_name and some data, write a
    function that
    • Creates a table in the database and name it from table_name (line_details in this case)
    • Takes in the data (in relational/ tabular form) and uploads it onto the line_details table

    *Note-
    Uploading data to SQLServer is not facilitated well using Pandas(requires SQLAlchemy engine/Sqlite), hence I am using SQLite3 to complete this project
    Hence, the credentials will contain the 'db location' instead of 'username','password','host','database' as required for tranditional db systems
     """

    if credentials == "" or table_name == "" :
        return -1

    try:
        obj = DB_Connection(credentials)
        conn = obj.getConnection()

        #Creating table
        if table_name == "line_details":
            # Since this data would not change that often, we can keep a check using change in length of DataFrame & length of number of records in 'line_details'(Would prevent duplication if data doesn't change)
            # Due to shortage of time, I have not implemented it.
            query = "CREATE TABLE IF NOT EXISTS '" + table_name +"' (line_name VARCHAR(255),description VARCHAR(255));"

            # Dumping the Relational Data into SQL Database
            data.to_sql(table_name, conn, if_exists="append", index=False)
        else:
            query = "CREATE TABLE IF NOT EXISTS '"+table_name+"' (id TEXT,time TEXT,late INTEGER,lat DECIMAL,lon DECIMAL,nextstop TEXT,source TEXT,dest TEXT);"
        obj.create_table(query)


    except Error as e:
        print(e)


def part1():
    """                     ********************** PART 1 **********************
    Step1 -> To create a function that fetches the JSON data from a given URL, Gets Rid of Metadata and and Transforms it into
             a relational form(Dataframe).
            # Create table 'line_details' in the database and upload the into the table
    Step2 -> To REUSE the functions created in part 1 to fetch, transform and upload inbound/outbound data for each line into their respective tables
    """
    # STEP 1 - Task 1
    url = "https://www.septastats.com/api/current/lines"
    data = fetchDataFromURL(url)

    # STEP 1 - Task 2
    headers = ['line_name', 'description']
    df = convertToRelationalFormat(data, 1, headers)  # 1 line is the header which can't be removed directly.

    # STEP 1 - Task 3
    database_credentials = {
        "host": "localhost",
        "user": "root",
        "password": "12345678",
        "database": "line_info"
    }

    # STEP 2 - TASK 1,2,3,4 [Reusing the API's created to collect, transform and store data for latest updates about inbound and outbound routes for all the train stations]
    table_name = "line_details"
    database_link = "/Users/Rahul/Desktop/tesla:line_info.db"
    uploadDataFromDataFrameToDatabase(database_link, table_name, df)

    train_lines = data['data'].keys()
    for line_name in train_lines:  # Time Complexiety O(2*n) ie O(n)    where n is number of train lines
        for direction in ["inbound", "outbound"]:
            direction_url_for_line = "https://www.septastats.com/api/current/line/%s/%s/latest" % (line_name, direction)
            line_data_for_direction = fetchDataFromURL(direction_url_for_line)
            table_name = "%s_%s" % (line_name, direction)

            headers = ["id", "time", "late", "lat", "lon", "nextstop", "source", "dest"]
            df = convertToRelationalFormat(line_data_for_direction, 0, headers)
            uploadDataFromDataFrameToDatabase(database_link, table_name, df)

def part2(N_minutes = 1):
    """
    Part 2 -> Goal is to repeat the operations in part1 in regular intervals of Given "n" minutes
    *Note:

    Possibilities:
    1> I could have normalized the database by reducing the redundancy and worked on indexing for increasing search times,
                    but due to shortage of time the choice of database that I have made, it would not be possible.
    2> Part2 function can be used as a decorator over 'part1()' to execute it with a delay of 'n' minutes in a continuous fashion.
    It can also be able to keep check of time required for part1() to complete.

    :param N_minutes:
    :return:
    """

    if N_minutes <= 0:
        return -1

    delay_provided = N_minutes *  60
    i = 0
    while True:
        part1()         # Part 1 is repeated called inside the infinite loop with a delay of 'n' minutes
        i+=1
        print("Iteration ",i)
        time.sleep(delay_provided)


# Unit Tests for convertToRelationalFormat()
def test_for_convertToRelationalFormat_with_empty_JSON():
    return convertToRelationalFormat(None,1,['names']) == -1

def test_for_convertToRelationalFormat_with_negative_value_of_N():
    N = -5
    return convertToRelationalFormat({'data':['tesla','bmw']},N,['names']) == -1

def test_for_convertToRelationalFormat_with_empty_headers():
    N = 1
    return convertToRelationalFormat({'data': ['tesla', 'bmw']}, N, []) == -1

def test_for_convertToRelationalFormat_with_empty_state_headers():
    N = 1
    return convertToRelationalFormat({'data': ['tesla', 'bmw']}, N, None) == -1

def test_for_fetchDataFromURL_with_empty_string_input():
    return fetchDataFromURL("") == -1

def test_for_fetchDataFromURL_with_empty_state():
    return fetchDataFromURL(None) == -1

def test_for_part2_with_Invalid_time_1():
    return part2(0) == -1

def test_for_part2_with_Invalid_time_2():
    return part2(-5) == -1

def test_for_uploadDataFromDataFrameToDatabase_with_empty_credentials():
    sample_df = pd.DataFrame(np.arange(24).reshape(8, 3),columns=['A', 'B', 'C'])
    return uploadDataFromDataFrameToDatabase("",'line_details',sample_df) == -1

def test_for_uploadDataFromDataFrameToDatabase_with_empty_tablename():
    sample_df = pd.DataFrame(np.arange(24).reshape(8, 3), columns=['A', 'B', 'C'])
    credentials = "/Users/Rahul/Desktop/tesla:line_info.db"
    return uploadDataFromDataFrameToDatabase(credentials, "", sample_df) == -1


def testing():
    functions = [

        test_for_convertToRelationalFormat_with_empty_JSON,
        test_for_convertToRelationalFormat_with_negative_value_of_N,
        test_for_convertToRelationalFormat_with_empty_headers,
        test_for_convertToRelationalFormat_with_empty_state_headers,
        test_for_fetchDataFromURL_with_empty_state,
        test_for_fetchDataFromURL_with_empty_string_input,
        test_for_part2_with_Invalid_time_1,
        test_for_part2_with_Invalid_time_2,
        test_for_uploadDataFromDataFrameToDatabase_with_empty_credentials,
        test_for_uploadDataFromDataFrameToDatabase_with_empty_tablename

    ]

    for fn in functions:
        if fn() == False:
            print("Error Occured in test case ",fn, "()! ")
        else:
            print("You passed the test case - ",fn,"()")



            
            
if __name__ == "__main__":


    testing()
    part2(1)        # TO PROVIDE THE DEFAULT DELAY FOR CONTINUOUSLY EXECUTING PART 1
