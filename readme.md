# Data Engineering 

Data Eng Sample task

## Getting Started

some of the asumption that are made for the task are as follows [Next steps and limitations]

1. Files in the given folder are static and list of files does not increase, in other words its a history log of files in the mentioned folders
2. The inital few lines of the script isnt parallel and logged as the location of the directory is set at this stage	
3. Files exist in [input/checks/right_to_work,input/checks/identity] for the appopriate date and time, no missing hours/date combination.
4. Data for individual application is present both hourly files if required.
5. Data in each file is correct w.r.t Date and hour of the file.
6. list for both json are up to date and have not reduced or changed to affect historical data.
7. File type for all input files[csv & json] are UTF-8 & LF
8. metadata json can be verified once by the main script but is not done in this case
9. The examples show version on which this code was tested.  

### Prerequisites

What things you need to run the script


1. Linux kernel version 5.3.0-59-generic
2. Python 3.6.9
3. All packages mentioned in the requir.txt are installed
4. Script is executed with rights to required folders


### Installing packages

A example that tell you how to get a env running.
The examples show version on which this code was tested.

check python version : Python 3.6.9

```
python3 --version
```
check pip version : pip 9.0.1

```
pip3 --version
```

requir.txt needs to be installed in the python env. 
kindly note that its not >= for version type 

```
pip3 install -r requir.txt 
```
### Run file 

A example  of how to run the file once the env is setup

-d is required this is the location of the input files 

example if files are in /home/usr1/folder1/input/checks you should write -d /home/usr1/folder1
```
python3 /home/usr1/main.py -d /location/of/all/files 
```

-p is optional it defaults to 3
```
python3 /home/usr1/main.py -d /location/of/all/files -p 6
```
### Expected folder structure

The Parent 'folder1' directory is structured as follows (assume local UNIX file system)

```
folder1
|__ input
|
|__ output
| |___ 2017-07-26-05.json
| |___ 2017-07-26-06.json
| |___ ...
|
|___logs
| |___ etl.log
|
|
```

The 'input' directory is structured as follows (assume local UNIX file system)

```
input
|__ metadata
| |__ applicant_nationality.json
| |__ applicant_employer.json
|
|__ checks
|___ right_to_work
| |___ 2017-07-26-05.csv
| |___ 2017-07-26-06.csv
| |___ ...
|
|___ identity
| |___ 2017-07-26-05.csv
| |___ 2017-07-26-06.csv
| |___ ...
```
