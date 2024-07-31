#!/bin/bash

cat <<EOF >> dagmaker.rc
#
# This is an example of a dagmaker.rc file
# The DAGMaker will look for this file in the same dir where it is running.
# If this file is not found by the DAGMaker.sh script,
# the dag will be created using some default options which might not be optimal.
# Please change this to reflect your desired configuration.
#
RNUM=$RNUM
PNUM=$PNUM
#
#### PLEASE READ: A word on season numbers #######
# Never use season numbers below 11; they are reserved for the SNe group
# Use a season number between 20 and 100 for testing and development
# Seasons from 100 so far correspond to GW events. E.g. seasons 1XX are for GW event 1, seasons 2XX for event 2, etc.
# As of the end of LIGO O2 we had 5 events, so the first O3 event will be season 600.
# Season 5000 is reserved for DES nightly difference imaging on the wide survey in Y5 (2017-2018)
# Season 6000 is reserved for DES nightly difference imaging on the wide survey in Y6 (2018-2019)
##############
SEASON=$SEASON

DIFFIMG_EUPS_VERSION=$EUPS_VERSION

# WRITEDB should be off for initial testing, but on for any production running.
WRITEDB=on

# When true, DAGMaker will delete a pre-existing mytemp dir for the exposure and rerun
RM_MYTEMP=true

JOBSUB_OPTS="--memory=2500MB --disk=70GB --cpu=1 --expected-lifetime=5h --email-to=alyssag94@brandeis.edu --disk=70GB --need-storage-modify /des/persistent/gw/exp --need-storage-modify /des/persistent/gw/forcephoto -e GFAL_PLUGIN_DIR=/usr/lib64/gfal2-plugins -e GFAL_CONFIG_DIR=/etc/gfal2.d"
JOBSUB_OPTS_SE="--memory=3600MB --disk=100GB --cpu=1 --expected-lifetime=5h --need-storage-modify /des/persistent/gw/exp --need-storage-modify /des/persistent/gw/forcephoto -e GFAL_PLUGIN_DIR=/usr/lib64/gfal2-plugins -e GFAL_CONFIG_DIR=/etc/gfal2.d"

RESOURCES="DEDICATED,OPPORTUNISTIC,OFFSITE"
IGNORECALIB=true
DESTCACHE=persistent

# optional additional arguments to SEdiff.sh, off by default
SEARCH_OPTS="-C"
TEMP_OPTS="-C -t"

### SCHEMA is the naming schema for the output files, not the DB schema. Valid values are gw and wsdiff.
SCHEMA="gw"
#t_eff cuts for each filter
TEFF_CUT_g=0.2
TEFF_CUT_i=0.3
TEFF_CUT_r=0.3
TEFF_CUT_Y=0.2
TEFF_CUT_z=0.3
TEFF_CUT_u=0.2
TWINDOW=15

#### ONLY use this option if you have nothing but late-time templates. It should be commented out for standard nightly diffim running.
#MIN_NITE=20170831
#MIN_NITE=20100101

#### Use only if you want to avoid using images taken after MAX_NITE as templates.
#MAX_NITE=20170901

#this variable tells DAGMaker to remove as templates any images that it sees as missing SE output. This should only
#only be used when you are certain you do not want to use any non-DES images and have already fetched all SE outputs from DESDM.

SKIP_INCOMPLETE_SE=false

### turn off header check for FIELD, OBJECT, TILING to save time. Can do that if you have already fixed the headers elsewhere (for example when copying from DESDM).
DO_HEADER_CHECK=0

# for 20810221
#Minimum stashcache revision version required for jobs to start. This is only useful for nightly difference imaging.
#STASHVER=30511
EOF