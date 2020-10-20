#!/bin/bash

#time go test -run 2B >xx
#echo $(tail -n 1 xxx | grep FAIL)
output=xx
for i in `seq 1 50`; do
    echo $i > $output
    go test -run "2A|B|C" >> $output
    tail -n 1 $output | grep FAIL
    if [ $? -eq 0 ]; then
        exit 1
    else
        tail -n 1 $output
    fi
done
