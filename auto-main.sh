#!/bin/bash

#must be logged in as des70 for jobsub to run
: '
ssh desgw@des70.fnal.gov
'

HOME_DIR="/data/des90.a/data/desgw/kquigley_testing/automation_pipeline"
cd $HOME_DIR

#set up arguments
SEASON="35"
EXP_NUMS='668441 668442' #668966 668967 671181 671182 668387 668388'

#begin pipeline
. ./auto-main_pipeline.sh -S $SEASON -E "$EXP_NUMS"

#to run on command line directly, use the same format:
: '
. ./auto-main_pipeline.sh -S 35 -E "668441 668442 668966 668967 671181 671182 668387 668388"
'