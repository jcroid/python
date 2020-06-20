# Data Engineering Example

Data eng task

## Getting Started

some of the asumption that are made for the task are as follows 
```
	1. Files in the given folder are static and list of files does not increase, in other words its a history log of files in the mentioned folders
	2. The inital few lines of the script isnt parallel and its logged as the location of the directory is set at this stage	
	3. Files exist in [input/checks/right_to_work,input/checks/identity] for the appopriate date and time, no missing hours/date combination.
	4. Data for individual application is present both hourly files if required.
	5. Data in each file is correct w.r.t Date and hour of the file.
	6. list for both json are up to date and have not reduced or changed to affect historical data.
	7. File type for all input files[csv & json] are UTF-8 & LF
```
### Prerequisites

What things you need to run the script

```
1. Linux kernel version 5.3.0-59-generic
2. Python 3.6.9
3. All packages mentioned in the requir.txt are installed
4. Script is executed with rights to required folders
```

### Installing

A examples that tell you how to get a env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo

## Running the tests

Explain how to run the automated tests for this system

### Break down into end to end tests

Explain what these tests test and why