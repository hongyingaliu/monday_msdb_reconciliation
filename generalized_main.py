import pyodbc, logging, os, csv, requests, json, yaml, threading, pandas as pd
from queue import Queue
from datetime import datetime

#Global Vars
SERVER_NAME = #SQL server domain name
DATABASE_NAME = #SQL database name
ROOTDIR = #output directory location for csvs/txts
DESCRIPTION = #description of the program
VERSION = #version of the program

#Monday.com API url
API_URL = "https://api.monday.com/v2/"
#insert Monday.com API key
VERSION_HEADERS = {'Content-Type': 'application/json',
                   'Authorization': }
#If needed, variables that should be taken out of the reconciliation can be listed in a list here
FILTERED_VARIABLES =

OUTPUT = pd.DataFrame()
DF_LOCK = threading.Lock()
THREAD_COUNT = #thread count desired for multithreading
log = logging.getLogger(__name__)

#read in column orders and mapping between databases from yaml file
with open(ROOTDIR + '\\monday_reconciliation.yaml', 'r') as file:
    data = yaml.safe_load(file)

#sql database column names
db_columns = data['labels']['db_columns']
#monday.com column names
mon_columns = data['labels']['mon_columns']
#mapping corresponding names from monday to sql
col_map = data['labels']['col_map']
#mapping corresponding names from sql to monday
rev_col_map = data['labels']['reverse_col_map']
#monday tag names for each column
mon_tags = data['labels']['mon_tags']
#read in monday api specific tags
mon_data = data['mon_data']

#functions to log info/data
def msg(msg):
    if PRINTSCREEN:
        print(msg)
    log.info(msg)

# Writes out the first messages for the log file to start
def startLog():
    logdir = ROOTDIR
    logfile = 'Logs\\Log.txt'

    if not os.path.exists(logdir):
        os.makedirs(logdir)
    logging.basicConfig(filename=logdir + '\\' + logfile, level=logging.DEBUG, format='%(message)s')
    msg('---')
    msg('Starting ' + DESCRIPTION + ' V' + VERSION)
    msg(datetime.now())
    msg('---')
    msg('')

#creates thread object for multithreading
class Multithread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name
    def run(self):
        #log start of thread
        msg('Starting '+self.name)
        execute(q, perform_comparison)
        #log completion of thread
        msg('Completed '+self.name)
        print('finished' + self.name)

#gets item from queue to feed into threads
def execute(q, func):
    while not q.empty():
        item = q.get(block=False)
        # call function to execute comparison
        func(item)
        q.task_done()

#take sql location and desired selection and return df
def sql_to_df(server_name, database_name, wanted_columns):
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=' + server_name + ';'
                          'Database=' + database_name + ';'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()

    selected = ', '.join(wanted_columns)
    #select relevant queries from SQL database
    query = '''SELECT ''' + selected + ''' '''
    df = pd.read_sql(query, conn)
    conn.close()
    msg('Created dataframe from' + query)
    msg(' ')
    return df

#returns monday ids from monday.com
def getmondayids():
    # make post request to Monday API, get ids
    id_param = {"query": "{boards(ids: ){items {id}}}"}
    response = requests.post(API_URL, headers=VERSION_HEADERS, params=id_param)
    r = response.json()
    df_mondayids = pd.DataFrame(r['data']['boards'][0]['items'])
    df_mondayids = df_mondayids.rename(columns={'id': 'Item ID'})
    # convert monday id datatypes to ints
    df_mondayids = df_mondayids.astype('int64')
    return df_mondayids

#returns ids present in both databases and ids missing from each side
def sortids(df1_noindex, df_mondayids):
    # separate sets of monday id, including all ids and missing ids
    # left is sql, right is monday.com
    leftids = df1_noindex
    rightids = df_mondayids
    allids = leftids.merge(rightids, how='outer', on='Item ID')
    sharedids = leftids.merge(rightids, how='inner', on='Item ID')
    missingidsL = pd.merge(leftids, rightids, on='Item ID', how="outer", indicator=True).query('_merge=="left_only"')
    missingidsR = pd.merge(leftids, rightids, on='Item ID', how="outer", indicator=True).query('_merge=="right_only"')
    return sharedids, missingidsL, missingidsR

#based on given ids, pulls all other info wanted from monday.com
def getmonday(chunked_ids):
    # change ids into strings to be used in query command
    string_ids = ', '.join([str(x) for x in chunked_ids])
    monday_tags = ', '.join(mon_tags)
    # monday query command
    full_param = {
        "query": "{boards(ids: ){items (ids: [" + string_ids + "]){ id, column_values (ids: ["+monday_tags+"]){title, text}}}}"}
    response = requests.post(API_URL, headers=VERSION_HEADERS, params=full_param)
    s = response.json()
    # flatten json
    s1 = s['data']['boards'][0]['items']
    dataframe2 = pd.json_normalize(s1, 'column_values', ['id'])
    # rotate title and text columns to correspond as rows with id
    dataframe2 = dataframe2.pivot(index='id', columns='title', values='text')

    # Reindex df2 columns to be in the right order
    columns_withid = mon_columns[:]     #make a copy to avoid appending to original list
    dataframe2_sorted = dataframe2.reindex(
        columns=columns_withid.append('Item ID'))
    dataframe2_sorted = dataframe2_sorted.astype({'Item ID': 'int64'})
    dataframe2_sorted = dataframe2_sorted.set_index('Item ID')
    dataframe2_sorted.index.name = 'Item ID'
    dataframe2_sorted = dataframe2_sorted.rename_axis(None, axis=1)
    # Convert datatypes to string
    dataframe2_sorted = dataframe2_sorted.convert_dtypes()
    dataframe2_sorted = dataframe2_sorted.astype('string')
    # fill all nas as blank
    dataframe2_sorted = dataframe2_sorted.fillna('')
    return dataframe2_sorted

#chunks list of ids into 100s
def getchunk(sharedids):
    if sharedids.shape[0] < 100:
        chunk = sharedids
    else:
        chunk = sharedids.iloc[0:100]
        sharedids = sharedids.iloc[101:]
    chunked_ids = list(chunk['Item ID'])
    return chunked_ids, sharedids

#compares two values for a row(id) and column
def check_item(col, item_id, b):
    #b is from MON
    #From DB
    a = dataframe1_sorted.loc[item_id, col]
    global OUTPUT
    #ignore suffixes that aren't labels on monday.com
    if (col == ) and (a.upper() in FILTERED_VARIABLES):
        return None
    #account for accidental space (from SQL) and decimal differences (from monday)
    elif (a.upper() == b.upper()):
        return None
    #if the values are not the same
    elif a != b:
        if (a != '') and (b == '') and (col in  mon_data['Columns'].keys()):
            #row = dataframe1_sorted.loc[item_id]
            #print(row + 'MON')
            mondayupdates(b, item_id, col)
            return None
            #msg('Item ID: '+item_id+' Column: '+col+' SQL: '+a+' MON: '+b)
        elif (a == '') and (b != '') and (col not in ['City', 'State', 'Zip', 'Project Manager', 'Studio', 'Landmark', 'DB ID']):
            db_update_str = dbupdates(b, col)
            return db_update_str
            #msg('Item ID: ' + item_id + ' Column: ' + col + ' SQL: ' + a + ' MON: ' + b)
        #elif:
            #logic for
        #returned dataframe
        else:
            df = pd.DataFrame([[item_id, col, a, b]], columns=['Item ID', 'Column', 'SQL','MON'])
            OUTPUT = OUTPUT.append(df)
            return None
    #else they are equal and do nothing

#function that calls comparison
def perform_comparison(chunked_ids):
    dataframe2_sorted = getmonday(chunked_ids)
    #give lock to thread
    DF_LOCK.acquire()
    for index, row in dataframe2_sorted.iterrows():
        update_str = 'UPDATE ' +
        set_str = 'SET '
        for col in row.index:
            value_str = check_item(col, index, row[col])
            if value_str is not None:
                set_str = set_str + value_str
        #if there's actually something to update to db, update at the end of row iteration
        if set_str != 'SET ':
            end_str = set_str[0:len(set_str)-2] + ' WHERE MONDAY_ID = ' + str(index)
            update_str = update_str + end_str
            msg(update_str)
            try:
                conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                              'Server=' + SERVER_NAME + ';'
                              'Database=' + DATABASE_NAME + ';'
                              'Trusted_Connection=yes;')
                cursor = conn.cursor()
                cursor.execute(update_str)
                conn.commit()
                conn.close()
            except Exception as e:
                msg(e)
        #release lock on thread
    DF_LOCK.release()

#function to push simple value onto monday
def simple_col_update(item_id, value, column_id):
    try:
        PARAMS = { "query" : "mutation {change_simple_column_value (board_id: , item_id :"+item_id+", column_id: \""+column_id+"\", value: \""+value+"\"){id}}"}
        response = requests.post(API_URL, headers=VERSION_HEADERS, params=PARAMS)
        #log response
        msg('monday id: '+item_id+' response')
    except Exception as e:
        msg('monday_id: ' + item_id + ' errored because'+ e + ', for' + column_id + ' with value ' + value)

#function to value mapped to categories/tags
def indexed_col_update(item_id, value, column_id, column_name):
    dict = mon_data['Columns'][column_name]['map']
    index = str(dict.get(value))
    try:
        PARAMS = { "query" : "mutation {change_column_value (board_id: , item_id :"+item_id+", column_id: \""+column_id+"\", value: \"{\\\"index\\\": "+index+"}\"){id}}"}
        response = requests.post(API_URL, headers=VERSION_HEADERS, params=PARAMS)
        #log response
        msg('monday id: '+item_id+' response')
    except Exception as e:
        msg('monday_id: ' + item_id + ' errored because' + e + ', for' + column_id + ' with value ' + value)

#grab monday info from yaml and make correct call for update
def mondayupdates(value, item_id, column_name):
    column_id = mon_data['Columns'][column_name]['id']
    montype = mon_data['Columns'][column_name]['type']
    item_id = str(item_id)
    if montype == 'simple':
        simple_col_update(item_id, value, column_id)      #push each value to monday.com
    elif montype == 'color':
        indexed_col_update(item_id, value, column_id, column_name)

#generate bulk update to database statement
def dbupdates(value, column_name):
    conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                          'Server=' + SERVER_NAME + ';'
                          'Database=' + DATABASE_NAME + ';'
                          'Trusted_Connection=yes;')
    cursor = conn.cursor()
    column_name = rev_col_map[column_name]
    cursor.execute('''SELECT COLUMN_NAME, DATA_TYPE 
                      FROM INFORMATION_SCHEMA.COLUMNS
                      where TABLE_NAME = 'PROJECT' ''')
    datatype = cursor.fetchall()
    column_type = [item for item in datatype if item[0] == column_name][0][1] # getting database column type
    if column_type != 'int':
        value = "'"+value+"'"
    conn.close()
    db_update_str = column_name + ' = ' + value + ', '
    return db_update_str

#MAIN
if __name__ == '__main__':
    startLog()

    #setup dataframes and ids
    dataframe1= sql_to_df(SERVER_NAME, DATABASE_NAME, db_columns)
    dataframe1_sorted = dataframe1.rename(columns=col_map)
    dataframe1_sorted = dataframe1_sorted.set_index('Item ID')
    dataframe1_sorted = dataframe1_sorted.convert_dtypes()
    dataframe1_sorted = dataframe1_sorted.astype('string')
    dataframe1_sorted = dataframe1_sorted.fillna('')

    df1_noindex = sql_to_df(SERVER_NAME, DATABASE_NAME, ['MONDAY_ID'])
    df1_noindex = df1_noindex.rename(columns=col_map)
    df_mondayids = getmondayids()
    sharedids, missingidsL, missingidsR = sortids(df1_noindex, df_mondayids)

    #remove past outputs
    if os.path.exists(ROOTDIR + '\\Logs\\reconciliation.txt'):
        os.remove(ROOTDIR + '\\Logs\\reconciliation.txt')
    if os.path.exists(ROOTDIR + '\\Logs\\missingids.txt'):
        os.remove(ROOTDIR + '\\Logs\\missingids.txt')

    #iterate over chunks
    chunk_number = (sharedids.shape[0]) // 100
    chunk_number = int(chunk_number)

    #fill queue with chunks
    msg('Filling Queue')
    q = Queue(maxsize=0)
    for i in range(chunk_number):
        chunked, sharedids = getchunk(sharedids)
        q.put(chunked)

    #start threads for multithread comparison
    threads = []
    msg('---')
    for i in range(THREAD_COUNT):
        worker = Multithread('Thread' + str(i + 1))
        worker.start()
        threads.append(worker)
    #stop here until queue is empty
    q.join()
    for worker in threads:
        worker.join()

    #format output for csv
    msg('output length: ' + str(len(OUTPUT)))
    if OUTPUT.empty == False:
        OUTPUT['Column'] = pd.CategoricalIndex(OUTPUT['Column'], ordered=True, categories=mon_columns)
        OUTPUT = OUTPUT.sort_values('Column')
        OUTPUT = OUTPUT.drop_duplicates()
    # Output comparison/inconsistencies
    msg('Outputted reconciliation')
    OUTPUT.to_csv(ROOTDIR + '\\Logs\\reconciliation.txt', mode='w', index=False)
    # Output missing monday ids
    msg('Outputted missing ids')
    missingidsL.to_csv(ROOTDIR + '\\Logs\\missingids.txt', mode='w', index=False)
    missingidsR.to_csv(ROOTDIR + '\\Logs\\missingids.txt', mode='a', index=False, header=False)
    msg('------------------------------')
    logging.shutdown()
