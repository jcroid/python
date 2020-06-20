######################
# import all packages
#######################
import pandas as pd
import numpy as np
import os
import json
import logging
from multiprocessing import Pool
import argparse


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


def lflverify(mylist, nameofjson):
    """this is a function that does a detailed check of list of list 
    it also verifies the type of var inside the child list for 
    this example it is [int,str]
    it return zero var if 0 then error if 1 then passed 
    """
    mainlist = 1 if isinstance(mylist, list) else 0

    try:
        childlist = [1 for cmylist in mylist if isinstance(cmylist, list)]
        childlist = 1 if len(childlist) == len(mylist) else len(childlist)

        listtype = [1 for cmylist in mylist if isinstance(
            cmylist[0], int) and isinstance(cmylist[1], str)]
        listtype = 1 if len(listtype) == len(mylist) else len(listtype)

        verify = mainlist + childlist + listtype
        if verify == 3:
            zero = 1
            logging.info("Hour " + nameofjson + " schema type is_verified")
        else:
            zero = 0
            logging.error(
                "Hour " + nameofjson + " schema type not as defined")

    except Exception as e:
        zero = 0
        logging.error("Hour " + nameofjson + " " +
                      str(e) + " schema type not as defined")

    return zero


def child(csvfile):
    # start timer
    t1 = pd.Timestamp.now()
    # print logging start
    logging.info("Hour " + csvfile.split('.')[0] + " ETL start.")

    ############list of var ##########

    # names of columns
    rtw = ["unix_timestamp", "applicant_id",
           "id_employer", "id_nationality", "is_eligble"]
    identity = ["unix_timestamp", "applicant_id", "is_verified"]

    filendjson = 'output/' + csvfile.split('.')[0] + '.json'

    mdemp = "input/metadata/applicant_employer.json"

    mdnat = "input/metadata/applicant_nationality.json"

    chrw = "input/checks/right_to_work/" + csvfile
    chid = "input/checks/identity/" + csvfile

    ###############################
    # Read files from files for the script and verify the data
    ###############################
    try:
        a = pd.read_csv(chrw, sep=',', names=rtw)

        # verify the csv file type
        dtypes = np.dtype([(rtw[0], np.int64), (rtw[1], np.int64), (rtw[
                          2], np.int64), (rtw[3], np.int64), (rtw[4], bool), ])
        data = np.empty(0, dtype=dtypes)
        df = pd.DataFrame(data)

        if all(a.dtypes) != all(df.dtypes):
            logging.error("Hour " + chrw.split('/')
                          [2] + "-" + csvfile + " columns type not as defined")
            return
            raise SystemExit('csv type not vaild')

        b = pd.read_csv(chid, sep=',', names=identity)

        # verify the csv file type
        dtypes = np.dtype(
            [(identity[0], np.int64), (identity[1], np.int64), (identity[2], bool), ])
        data = np.empty(0, dtype=dtypes)
        df = pd.DataFrame(data)

        if all(b.dtypes) != all(df.dtypes):
            logging.error("Hour " + chid.split('/')
                          [2] + "-" + csvfile + " columns type not as defined")
            return
            raise SystemExit('csv type not vaild')

        with open(mdemp) as data_file:
            data = data_file.read()
            c = json.loads(data)

        # verify the json file type

        lflc = lflverify(mylist=c, nameofjson=mdemp.split('/')[2])

        if lflc == 0:
            return
            raise SystemExit('metadata schema not vaild')

        c = [["id_employer", "applicant_employer"]] + c
        c = pd.DataFrame(c, columns=c.pop(0))

        with open(mdnat) as data_file:
            data = data_file.read()
            d = json.loads(data)

        # verify the json file type

        lfld = lflverify(mylist=d, nameofjson=mdnat.split('/')[2])

        if lfld == 0:
            return
            raise SystemExit('metadata schema not vaild')

        d = [["id_nationality", "applicant_nationality"]] + d
        d = pd.DataFrame(d, columns=d.pop(0))

        logging.info("Hour " + csvfile.split('.')
                     [0] + " all files read and is_verified.")
    except Exception as e:
        logging.error("Hour " + "-" + csvfile + str(e) +
                      " error in reading files into child process")
        return
        raise SystemExit('read file error')
    ###############################
    # massage the data to make it ready to merge
    ###############################
    try:
        a['iso8601_timestamp'] = pd.to_datetime(a['unix_timestamp'], unit='s').astype(
            'datetime64[ns, Europe/London]').dt.strftime('%Y-%m-%dT%H:%M:%S')
        a['date'] = pd.to_datetime(
            a['iso8601_timestamp']).dt.strftime('%Y-%m-%d')
        a['hour'] = pd.to_datetime(a['iso8601_timestamp']).dt.strftime('%H')

        # delete the extra column
        a.drop(['unix_timestamp'], axis=1, inplace=True)

        b['iso8601_timestamp'] = pd.to_datetime(b['unix_timestamp'], unit='s').astype(
            'datetime64[ns, Europe/London]').dt.strftime('%Y-%m-%dT%H:%M:%S')
        b['date'] = pd.to_datetime(
            b['iso8601_timestamp']).dt.strftime('%Y-%m-%d')
        b['hour'] = pd.to_datetime(b['iso8601_timestamp']).dt.strftime('%H')

        # delete the extra column
        b.drop(['iso8601_timestamp', 'unix_timestamp'], axis=1, inplace=True)

        ###############################
        # merge all files to make it ready for export
        ###############################

        a = a.merge(c, on=['id_employer'])
        a = a.merge(d, on=['id_nationality'])

        # delete the extra column
        a.drop(['id_nationality', 'id_employer'], axis=1, inplace=True)

        merged = a.merge(b, how='left', on=['applicant_id', 'date', 'hour'])

        # delete the extra column
        merged .drop(['date', 'hour'], axis=1, inplace=True)
        # dont output columns to prejson format if nan
        merged = [{**x[i]} for i, x in merged.stack().groupby(level=0)]

        merged = [json.dumps(record) for record in merged]
        logging.info("Hour " + csvfile.split('.')
                     [0] + " massage and merge step complete")
    except Exception as e:
        logging.error("Hour " + "-" + csvfile + str(e) +
                      " error in massage and merge step")
        return
        raise SystemExit('error')
    ###############################
    # export to ndjson
    ###############################
    try:
        # output the ndjson file
        with open(filendjson, "w") as obj:
            for i in merged:
                obj.write(i + "\n")
    except Exception as e:
        logging.error("Hour " + "-" + csvfile + str(e) +
                      " error in file export")
        return
        raise SystemExit('read file error')
    # stop timer
    t2 = (pd.Timestamp.now() - t1).seconds

    logging.info("Hour " + csvfile.split('.')
                 [0] + " ETL complete,elapsed time: " + str(t2))
    return None


fnames = os.listdir(location + "/input/checks/right_to_work")
pool = Pool(processes=process)
pool.map(child, fnames, chunksize=1)
