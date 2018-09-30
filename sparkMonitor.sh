#!/bin/bash
python application_test.py -port $1
if [ $? ]
then
    python3 application_test.py -port $1
else
    exit 255
fi