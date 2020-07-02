#!/bin/bash

mkdir -p /tmp/count
# get current working dir of the script make that dir
cd "$(dirname "$0")"
dir=$(pwd)
echo $dir
#set dir to input folder
cd $dir/input
for f in checks/right_to_work/*.csv
do
	echo $f
	# this is the split the file and take the second part
	f="$(cut -d'/' -f3 <<<"$f")"
	# this ignores the file not preset error
	if [ -n "$(ls -A checks/identity/$f 2>/dev/null)" ]
	# it will only output in true condition
	then
		echo $f > /tmp/count/$f
	fi
done;