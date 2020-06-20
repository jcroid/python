######################
# import all packages
#######################
import pandas as pd
import os
import json
import logging
from multiprocessing import Pool
import argparse
import traceback

# collect all variables passed from the bash terminal into the script
parser = argparse.ArgumentParser(description='List of option for this file')
parser.add_argument('-p', type=int, default=3,
                    help='number of cores/thread,please enter a int >= 1')
parser.add_argument('-d', type=str, required=True,
                    help='full dir of the folder , /home/aws1 not /home/aws1/input')
args = parser.parse_args()
# assign the var passed
process = args.p
location = args.d

# change dir
os.chdir(location)

# setup the logging process
logging.basicConfig(level=logging.DEBUG, filename="logs/etl.log", filemode="a+",
                    format="[%(asctime)s] - %(levelname)s - %(message)s")


def child(csvfile):
    # start timer
    t1 = pd.Timestamp.now()
    # print logging start
    logging.info("Hour" + csvfile.split('.')[0] + "ETL start.")

    ############list of var ##########

    # names of columns
    checks = ["unix_timestamp", "applicant_id",
              "id_employer", "id_nationality", "is_eligble"]
    identity = ["unix_timestamp", "applicant_id", "is_verified"]

    filendjson = 'output/' + csvfile.split('.')[0] + '.json'

    mdemp = "input/metadata/applicant_employer.json"

    mdnat = "input/metadata/applicant_nationality.json"

    chrw = "input/checks/right_to_work/" + csvfile
    chid = "input/checks/identity/" + csvfile

    ###############################
    # Read files from dir for the script
    ###############################
    a = pd.read_csv(chrw, sep=',', names=checks)
    b = pd.read_csv(chid, sep=',', names=identity)

    with open(mdemp) as data_file:
        data = data_file.read()
        c = json.loads(data)

    with open(mdnat) as data_file:
        data = data_file.read()
        d = json.loads(data)

    ###############################
    a['iso8601_timestamp'] = pd.to_datetime(a['unix_timestamp'], unit='s').astype(
        'datetime64[ns, Europe/London]').dt.strftime('%Y-%m-%dT%H:%M:%S')
    a['date'] = pd.to_datetime(a['iso8601_timestamp']).dt.strftime('%Y-%m-%d')
    a['hour'] = pd.to_datetime(a['iso8601_timestamp']).dt.strftime('%H')

    # delete the extra column
    a.drop(['unix_timestamp'], axis=1, inplace=True)

    b['iso8601_timestamp'] = pd.to_datetime(b['unix_timestamp'], unit='s').astype(
        'datetime64[ns, Europe/London]').dt.strftime('%Y-%m-%dT%H:%M:%S')
    b['date'] = pd.to_datetime(b['iso8601_timestamp']).dt.strftime('%Y-%m-%d')
    b['hour'] = pd.to_datetime(b['iso8601_timestamp']).dt.strftime('%H')

    # delete the extra column
    b.drop(['iso8601_timestamp', 'unix_timestamp'], axis=1, inplace=True)

    c = [["id_employer", "applicant_employer"]] + c
    c = pd.DataFrame(c, columns=c.pop(0))
    d = [["id_nationality", "applicant_nationality"]] + d
    d = pd.DataFrame(d, columns=d.pop(0))

    a = a.merge(c, on=['id_employer'])
    a = a.merge(d, on=['id_nationality'])

    # delete the extra column
    a.drop(['id_nationality', 'id_employer'], axis=1, inplace=True)

    merged = a.merge(b, how='left', on=['applicant_id', 'date', 'hour'])

    # delete the extra column
    merged .drop(['date', 'hour'], axis=1, inplace=True)

    merged = [{**x[i]} for i, x in merged.stack().groupby(level=0)]

    merged = [json.dumps(record) for record in merged]

    # output the ndjson file
    with open(filendjson, "w") as obj:
        for i in merged:
            obj.write(i + "\n")

    # stop timer
    t2 = (pd.Timestamp.now() - t1).seconds

    logging.info("Hour" + csvfile.split('.')
                 [0] + "ETL complete,elapsed time:" + str(t2))
    return None

try:
    fnames = os.listdir(location + "/input/checks/right_to_work")
    pool = Pool(processes=process)
    pool.map(child, fnames, chunksize=1)
except Exception:
    logging.error("Main" + traceback.print_exc() + "ETL did not start")
