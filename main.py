######################
# import all packages
#######################

from multiprocessing import Pool
import argparse
from functools import partial
from helperf import *


def main():
    # collect all variables passed from the bash terminal into the script
    parser = argparse.ArgumentParser(
        description='List of option for this file')
    parser.add_argument('-p', type=int, default=3,
                        help='number of cores/thread,please enter a int >= 1')
    parser.add_argument('-d', type=str, required=True,
                        help='full dir of the folder , /home/aws1 not /home/aws1/input')
    args = parser.parse_args()

    # assign the var passed
    process = args.p
    location = args.d

    func = partial(child, location)

    fnames = os.listdir(location + "/input/checks/right_to_work")
    pool = Pool(processes=process)
    pool.map(func, fnames, chunksize=1)

main()
print("run main func")
