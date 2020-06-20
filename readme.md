# Data Engineering Example

Data eng task

## Getting Started

some of the asumption that are made for the task are as follows 
```
	1. Files in the given folder are static and list of files does not increase, in other words its a history log of files in the mentioned folders
	2. The inital few lines of the script isnt parallel and logged as the location of the directory is set at this stage	
	3. Files exist in [input/checks/right_to_work,input/checks/identity] for the appopriate date and time, no missing hours/date combination.
	4. Data for individual application is present both hourly files if required.
	5. Data in each file is correct w.r.t Date and hour of the file.
	6. list for both json are up to date and have not reduced or changed to affect historical data.
	7. File type for all input files[csv & json] are UTF-8 & LF
	8. metadata json can be verified once by the main script but is not done in this case 
```
### Prerequisites

What things you need to run the script

```
1. Linux kernel version 5.3.0-59-generic
2. Python 3.6.9
3. All packages mentioned in the requir.txt are installed
4. Script is executed with rights to required folders
```

### Installing packages

A examples that tell you how to get a env running

check python version : Python 3.6.9

```
python3 --version
```
check pip version : pip 9.0.1

```
pip3 --version
```

requir.txt needs to be installed in the python env
kindly note that its not >= for version type 

```
pip3 install -r requir.txt 
```
