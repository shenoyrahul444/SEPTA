
from DB_Connection  import DB_Connection      # Imported from another local module to handle database interactions
import json
def processDBdata(query):
    database_link = "/Users/Rahul/Desktop/GitHub/SEPTA_VIZ/tesla:line_info.db"
    obj = DB_Connection(database_link)
    conn = obj.getConnection()
    return obj.execute(query)

    # Creating table
    # if table_name == "line_details":
    #     # Since this data would not change that often, we can keep a check using change in length of DataFrame & length of number of records in 'line_details'(Would prevent duplication if data doesn't change)
    #     # Due to shortage of time, I have not implemented it.
    #     query = "CREATE TABLE IF NOT EXISTS '" + table_name + "' (line_name VARCHAR(255) ,description VARCHAR(255),PRIMARY KEY(line_name) );"
    #
    # else:
    #     # query = "CREATE TABLE IF NOT EXISTS '" + table_name +"' (id TEXT ,time TEXT ,late INTEGER,lat DECIMAL,lon DECIMAL,nextstop TEXT,source TEXT,dest TEXT);"
    #     query = "CREATE TABLE IF NOT EXISTS '" + table_name + "' (id TEXT ,time TEXT ,late INTEGER,lat DECIMAL,lon DECIMAL,nextstop TEXT,source TEXT,dest TEXT, PRIMARY KEY (id,time));"
    #     # , PRIMARY KEY (id,time)

if __name__ == "__main__":

    # Get line names
    lines_query = "select line_name from line_details"
    lines = processDBdata(lines_query)
    for i,line in enumerate(lines):
        lines[i] = line[0]



    # Convert all the train data to GeoJSON format (Group ID, Group
    order_ctr= {}
    master = []

    for line in lines:
        for direction in ["inbound","outbound"]:
            table_name = line+"_"+direction
            query = "select *  from '"+table_name+"'  order by id, DATETIME(time);"
            records = processDBdata(query)
            for record in records:
                train_id,timestamp,late,lat,lon,nextStop,src,dest = record
                if train_id in order_ctr:
                    order_ctr[train_id] += 1
                else:
                    order_ctr[train_id] = 1

                master.append({
                    "line_group":line,
                    "train_id":train_id,
                    "order":order_ctr[train_id],
                    "timestamp":timestamp,
                    "late":late,
                    "lat":lat,
                    "lon": lon,
                    "nextStop":nextStop,
                    "source":src,
                    "destination":dest
                })
    print("********Master********** \n",master)
    file_name = "data/train_lines_data.json"
    with open(file_name, 'w') as outfile:
        json.dump(master, outfile, indent=2)
        print("Created %s data file" % (file_name))


