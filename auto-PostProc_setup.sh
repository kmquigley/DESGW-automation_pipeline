#!/bin/bash

source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh   #enables the setup command

setup -j pandas 0.15.2+3
setup finalcut Y6A1+2

setup easyaccess
setup extralibs 1.1

setup -j joblib -Z /cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/packages

#setup ftools v6.17 
#export HEADAS=$FTOOLS_DIR

#setup autoscan

#setup psycopg2 2.4.6+8

#setup perl 5.18.1+6 # || exit 134
#setup diffimg gw8
#setup astropy 0.4.2+6
#setup -j healpy 1.8.1+3

cd ./Post-Processing
python ./auto-PostProc.py --ligoid $LIGOID --triggerid $TRIGGERID --triggermjd $TRIGGERMJD &> ./postproc_out.out