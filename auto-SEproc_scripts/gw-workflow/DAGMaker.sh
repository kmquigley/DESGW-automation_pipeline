#!/bin/bash

umask 002

##export LD_PRELOAD=/usr/lib64/libpdcap.so.1

# give an exposure number and generate a DAG to do our full chain. The structure is like so: 

# 1) Given exposure number, calculate which templates we need to run SE procoessing for. Store those in some list.
#
#  2) Check to see which of the templates has already been through SE processing, if any. Remove them from the list.
#
# 3) Set up standard output directory space in dCache and give appropriate permissions to the dirs
#
# 4) Start making the first stage of the DAG
# a) Create a set of parallel jobs that does the SE processing for the new exposure and all of its dependencies that haven't run yet (the list from steps 1-2.)
#    If all templates have already been through SE processing, this section will consist only of the SE processing for the new exposure
# b) at the end add a "dummy" serial jbos that does nothing except set up the proper dependency. It could send a mail or something saying that the SE steps are done
#
# 5) Make the second stage of the DAG
# a) now create 60 parallel jobs (one per chip) and run the full diffimg pipeline within that job
# b) each parallel job runs the same script, but takes the appropriate chip number (templates too?) as arguments
#
# 6) final stage of the DAG: single Runmon job to finalize everything
#
#
#   Visually, DAG is like this:
#
#
#   SEnewexp  SEtemplate1  SEtemplate2 ... SEtemplateN     (could be only SNnewexp if templates are already done)
#     \            |            |              /
#      \           |            |             /
#       \          |            |            /
#        \         |            |           /
#                
#                   dummy job
#                 /     |     \
#                /      |      \
#     Diffimg chip1   .....    Diffimg chip 62              
#                \      |      /
#                 \     |     /
#                RUNEND_monDiffimg  
#
#
#
#
#.

##### helper functions ######

fetch_noao() {

# first we need the RA and DEC of the image in question
full_imageline=$(egrep "^\s{0,}${overlapnum}" exposures_${BAND}.list)
#full_imageline=$(awk '($1=='${EXPNUM}') exposures_${BAND}.list')

imageline=$(echo $full_imageline | awk '{print $4,$5}' )
PROPID=$( echo  $full_imageline | awk '{print $8}' )
SEARCHRA=`echo $imageline | cut -d " " -f 1`
SEARCHDEC=`echo $imageline | cut -d " " -f 2`

fetchurl="http://nsaserver.sdm.noao.edu:7001/?instrument=decam&obstype=object&proctype=raw&date=${overlapnite:0:4}-${overlapnite:4:2}-${overlapnite:6:2}&PROPOSAL=${PROPID}&FORMAT=image/fits&RELEASE_STATUS=public"

echo "fetchurl = $fetchurl"
curl -s $fetchurl -o votable_${overlapnum}.xml

sed -i -e s/datatype=\"date\"/datatype=\"char\"/ -e 's/\,/ /g' votable_${overlapnum}.xml

cat <<EOF > get_images_${overlapnum}.py
#!/usr/bin/python
from astropy.io.votable import parse_single_table
from subprocess import Popen
import sys
import os
import math
import subprocess
table=parse_single_table("votable_${overlapnum}.xml")
RA =  $SEARCHRA
DEC = $SEARCHDEC
SEARCHEXP = $overlapnum
j=0
k=0
not_end=1
s_url =[]
s_crval = []
dists = []
while not_end:
    s_url0 = None
    s_crval0 = None
    try:
       s_url0=table.array['access url'][j]
       s_crval0=table.array['CRVAL'][j]
#       print s_url0, s_crval0
    except IndexError:
       not_end=0
#    print s_url0
    if s_url0 != None:    
        s_url1=s_url0.replace("7006","7003")
#        s_url1=s_url0.replace("7506","7003")
        i=s_url1.find("&extension")
        s_url2=s_url1[0:i]
        RADIFF = float(s_crval0[0]) - RA
        # beware the wraparound problem...
        if RADIFF > 180.0 : RADIFF -= 360.0 
        if RADIFF < -180.0 : RADIFF += 360.0 
        DECDIFF = float(s_crval0[1]) - DEC
        if abs(RADIFF) <= 0.1 and abs(DECDIFF) <= 0.1 :
            if s_url2 not in s_url:
                dist=math.hypot(RADIFF,DECDIFF)
                insert_index = 0
                for ii in range(0,len(dists)):
                    if dists[ii] < dist: insert_index +=1
                dists.insert(insert_index, dist)
                s_url.insert(insert_index, s_url2)
                s_crval.insert(insert_index, s_crval0)
 #               print j,  s_url[k], s_crval[k]
                k=k+1
    j=j+1
#
##download files using curl
n_files=0
n_files=k
if n_files < 1:
    print('Error, no images to download!\n')
    sys.exit(1)
print "There are %d image files"  % n_files 
for ifile in range(0,n_files):
     i_fname=s_url[ifile].find("=")
     fname=s_url[ifile][i_fname+1:]
     print "\n **********retreiving image %d " % ifile, s_url[ifile]
     finfile=s_url[ifile]
     print finfile, fname
     expstring=""
     try:
         os.system("curl "+ finfile +" -o " + fname)
     except:
         print("Error downloading file from noao!\n" )
         continue
     #### check if this is really the image that we want
     funhead=subprocess.Popen(["/home/s1/marcelle/bin/funhead",fname],stdout=subprocess.PIPE)
     grepcmd=subprocess.Popen(["grep","EXPNUM"],stdin=funhead.stdout,stdout=subprocess.PIPE)
     funhead.stdout.close()
     expstring=grepcmd.communicate()[0]
     try:
         expstring=expstring.split()[2]
     except:
         print("No EXPNUM in header\n")
         expstring="-1"
     print expstring + "\n"  
     if int(expstring) == SEARCHEXP :
         try:
             os.system("cp " + fname + " /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_$(printf %08d ${overlapnum}).fits.fz")
             sys.exit(0)
         except:
             print("Error copying file into dCache!\n")
         finally:
             os.system("rm " + fname)
             break # we found the right exposure; no point in looking at the others

     os.system("rm " + fname)   
sys.exit(1)
EOF

python get_images_${overlapnum}.py

return $?

}

check_header() {

# we need to see if the "OBJECT" field in the image header in dCache contains the word "hex". If it does not then we need to 
# replace that field in the header with "DESGW hex $FIELD tiling 1"
    
    IMGOBJECT=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz OBJECT)
    IMGTILING=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz TILING)
    imageline=$(awk '($1=='${EXPNUM}') {print $4,$5}'  exposures_${BAND}.list)
    SEARCHRA=`echo $imageline | cut -d " " -f 1`
    SEARCHDEC=`echo $imageline | cut -d " " -f 2 | sed s/+//`
    RA10=$(echo "${SEARCHRA}*10" | bc | cut -d "." -f 1)
    if [ -z "$RA10" ] ; then RA10=0 ; fi
    DEC10=$(echo "$SEARCHDEC * 10" | bc | cut -d "." -f 1)
    if [ -z "$DEC10" ] ; then DEC10=0 ; fi
    if [ $DEC10 -ge 0 ]; then
    DEC10="+${DEC10}"
    fi
    
    NEWFIELD="WS${RA10}${DEC10}"
    NEWTILING=1
    NEWOBJECT="DESWS hex $NEWFIELD tiling $NEWTILING"
    
    echo "OBJECT = '${NEWOBJECT}'/ Object name" > editfile_$$
    echo "FIELD = '${NEWFIELD}'" >> editfile_$$
    echo "TILING = 1" >> editfile_$$
    
   ### first copy the file down
    $COPYDCMD /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz ./ && rm -f /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz
    chmod u+w DECam_`printf %08d ${EXPNUM}`.fits.fz
    for hdr in {1..9} {10..70} 
    do
	fthedit "DECam_$(printf %08d ${EXPNUM}).fits.fz[${hdr}]"  @editfile_$$ || echo "Error running fthedit for  DECam_`printf %08d ${EXPNUM}`.fits.fz[${hdr}]"
    done
    rm editfile_$$
    
    $COPYCMD DECam_`printf %08d ${EXPNUM}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz
    
    if [ $? -eq 0 ]; then
    rm DECam_`printf %08d ${EXPNUM}`.fits.fz
    else
    echo "Error copying edited file DECam_`printf %08d ${EXPNUM}`.fits.fz back to dCache!"
    rm DECam_`printf %08d ${EXPNUM}`.fits.fz
    exit 1 
    fi
    
} ### END check_header

#### making the dag ####

# read the search exposure number
if [ $# -lt 1 ]; then echo "Error, an exposure number must be supplied" ; exit 1; fi
ALLEXPS="$@"
# {
echo "All the exposures are:"
echo ${ALLEXPS[*]}
ALLEXPS=($(echo ${ALLEXPS[*]}| tr " " "\n" | sort -n))
echo "Now, sorted, they are:"
echo ${ALLEXPS[*]}
# } Added by NS on 25.06.2021
EXPNUM=$1
[[ $EXPNUM =~ ^[0-9]+$ ]] || { echo "Error, exposure number must be a number; you entered $EXPNUM." ; exit 1; }
echo "EXPNUM = $EXPNUM"

# check that all necessary files exist:
requiredfiles=( ~/.pgpass ~/.desservices.ini ~/.wgetrc-desdm )
for requiredfile in ${requiredfiles[*]}
do
    if [ ! -f $requiredfile ] ; then echo "Error: $requiredfile not found." ; exit 2 ; fi
done

# check also the optional files:
optionalfiles=( ./dagmaker.rc )
if [ ! -f $optionalfile ] ; then echo "Warning: $optionalfile not found." ; fi 

# set default parameters
RNUM="2"
PNUM="01"
SEASON="11"
JOBSUB_OPTS="--memory=3000MB --expected-lifetime=medium --cpu=1 --mail_on_error --email-to=kherner@fnal.gov --need-storage-modify /des/persistent/gw/exp --need-storage-modify /des/persistent/gw/forcephoto"
DIFFIMG_EUPS_VERSION="gw8"
WRITEDB="off"
RM_MYTEMP="false"
IGNORECALIB="false"
DESTCACHE="persistent"
SEARCH_OPTS=""
SCHEMA="gw"
TWINDOW=30.0
#{
TWINDOW_DISTINCT=0
TWINDOW_BEFORE=0
TWINDOW_AFTER=0
#} Added by NS on 25.06.2021
TEFF_CUT=0.0
TEFF_CUT_g=0.0
TEFF_CUT_i=0.0
TEFF_CUT_r=0.0
TEFF_CUT_Y=0.0
TEFF_CUT_z=0.0
SKIP_INCOMPLETE_SE="false"
# Added a default to min_nite, on 2017 dec 1st
MIN_NITE=20100101
MAX_NITE=99999999
DO_HEADER_CHECK=1

STASHVER=""

# overwrite defaults if user provides a .rc file
DAGMAKERRC=./dagmaker.rc
if [ -f $DAGMAKERRC ] ; then
    echo "Reading params from config file: $DAGMAKERRC"
    source $DAGMAKERRC
fi

# set processing versions
procnum="dp$SEASON"
rpnum="r"$RNUM"p"$PNUM
if [ ! -z "$STASHVER" ]; then
    STASHVER="&&(TARGET.CVMFS_des_osgstorage_org_REVISION>=${STASHVER})"
fi
echo $STASHVER

# print params used in this run
echo "----------------"
echo "SEASON = $SEASON => DIFFIMG proc. version is $procnum"
echo "RNUM = $RNUM , PNUM = $PNUM  => SE proc. version is $rpnum"
echo "WRITEDB = $WRITEDB (default is WRITEDB=off; set WRITEDB=on if you want outputs in db)"  
echo "RM_MYTEMP = $RM_MYTEMP"  
echo "IGNORECALIB = $IGNORECALIB (default is false)"
echo "JOBSUB_OPTS = $JOBSUB_OPTS"
echo "DIFFIMG_EUPS_VERSION = $DIFFIMG_EUPS_VERSION"
echo "DESTCACHE = $DESTCACHE"
echo "SCHEMA = $SCHEMA"
echo "TWINDOW = $TWINDOW"
echo "MIN_NITE = $MIN_NITE"
echo "MAX_NITE = $MAX_NITE"
echo "----------------"

# if there is a pre-existing mytemp dir for this exposure AND the RM_MYTEMP flag is set, remove the dir
if [ -d mytemp_$EXPNUM ]; then
    echo "RM_MYTEMP flag was set, so we are deleting the pre-existing mytemp_$EXPNUM dir"
    rm -r mytemp_$EXPNUM
fi

# read in SE_blacklist.txt
blexps=$(cat SE_blacklist.txt)

# see if wre going to be doing on-the-fly SNSTAR and SNVETO catalogs from the templates. Based on the content of  MAKESCRIPT_DIFFIMG_TEMPLATE.INPUT
# this writes to the RUN_DIFFIMG_PIPELINE $SNSTAR_FILENAME variable, which should be exported to the RUN02 script when it is called (and then RUN02 should do the eval of $SNSTAR_FILENAME since the $CCDNUM_LIST is finally known at that point)
SNSTAR_OPTS=""
SNVETO_OPTS=""
SNSTAR_FILENAME=`egrep "^\s*SNSTAR_FILENAME" MAKESCRIPT_DIFFIMG_TEMPLATE.INPUT | cut -d ":" -f 2- | sed -r -e  "s/\#.*//" -e "s/^\ *//" -e "s/(\ )*$//" | sed -e "s/THEEXP/${EXPNUM}/" -e "s/THERNUM/${RNUM}/" -e "s/THEPNUM/${PNUM}/" -e "s/THECCDNUM/\\\${CCDNUM_LIST}/"`
SNVETO_FILENAME=`egrep "^\s*SNVETO_FILENAME" MAKESCRIPT_DIFFIMG_TEMPLATE.INPUT | cut -d ":" -f 2- | sed -r -e  "s/\#.*//" -e "s/^\ *//" -e "s/(\ )*$//" | sed -e "s/THEEXP/${EXPNUM}/" -e "s/THERNUM/${RNUM}/" -e "s/THEPNUM/${PNUM}/" -e "s/THECCDNUM/\\\${CCDNUM_LIST}/"`

if [ -z "$SNSTAR_FILENAME" ]; then unset SNSTAR_FILENAME ; fi
## Above line was added on 20171208 by Ken suggestion to fix an error in Francisco submission
### dummy job
cat <<EOF > dummyjob.sh
echo "I do not actually do anything except say hello."
exit 0
EOF
chmod a+x dummyjob.sh

echo "set up environment, and handy commands"

source /cvmfs/des.opensciencegrid.org/ncsa/centos7/finalcut/Y6A1+2/eups/desdm_eups_setup.sh
# source /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh 
export EUPS_PATH=/cvmfs/des.opensciencegrid.org/ncsa/centos7/finalcut/Y6A1+2/eups/packages:/cvmfs/des.opensciencegrid.org/eeups/fnaleups:/cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/packages
setup oracleclient
setup wcslib
setup cfitsio
setup cfitsio_shared
setup gsl
setup diffimg gw8
setup CoreUtils 1.0.1+0
setup wcstools 3.9.6+0
export WCS_INC=-I${WCSLIB_DIR}/include/wcslib
export SNANA_DIR=/cvmfs/des.opensciencegrid.org/eeups/fnaleups/Linux64/SNANA/v11_03e
export EUPS_PATH=${EUPS_PATH}:/cvmfs/des.opensciencegrid.org/eeups/fnaleups
setup -j ftools v6.17
export HEADAS=$FTOOLS_DIR
export PATH=$DIFFIMG_DIR/bin:$PATH
export PATH=${WCSTOOLS_DIR}/bin:${PATH}
export DIFFIMG_HOST=FNAL
#for IFDH
export EXPERIMENT=des
export PATH=${PATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/cpn/v1_7/NULL/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v2_6_18/Linux64bit-3-10-2-17/bin
export PYTHONPATH=${PYTHONPATH}:/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v2_6_18/Linux64bit-3-10-2-17/lib/python
export IFDHC_CONFIG_DIR=/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc_config/v2_6_18/NULL
export IFDH_NO_PROXY=1
export IFDH_CP_UNLINK_ON_ERROR=1
export IFDH_CP_MAXRETRIES=2

if [ ! -d syspfiles_$$ ]; then
    mkdir syspfiles_$$
    ln -s ${FTOOLS_DIR}/syspfiles/* syspfiles_$$
fi
export PFILES=$PWD/syspfiles_$$


# setup handy  commands
COPYCMD="ifdh cp"
COPYDCMD="ifdh cp -D"
CHMODCMD="ifdh chmod 775"
RMCMD="ifdh rm"
#allow people logged in as desgw to do a straight cp to /pnfs to avoid long lock times
#if [ "${USER}" == "desgw" ]; then
#COPYCMD="cp"
#COPYDCMD="cp"
#CHMODCMD="chmod g+w"
#RMCMD="rm -f"
#fi

echo "prep the list files"

# create the exposures.list file, if it doesn't already exist
if [ ! -f exposures.list ]; then
    ./getExposureInfo.sh
    # and remove the diff.list2 to make sure it stays in sync with the new .list file
    rm -f ./mytemp_${EXPNUM}/KH_diff.list2
else
    for ifilter in u g r i z Y
    do
    	if [ ! -f exposures_${ifilter}.list ]; then
    	    awk '($6 == "'$ifilter'")' exposures.list > exposures_${ifilter}.list
    	fi
    done
fi
BAND=$(awk '($1=='${EXPNUM}') {print $6}' exposures.list)
if [ -z "${BAND}" ]; then
    echo "Error with setting band. Check exposures.list to see if there is a problem with this exposure. Exiting..."
    exit 1
fi

# set the TEFF cut based on the band
case $BAND in
    g)
	TEFF_CUT=$TEFF_CUT_g
	;;
    i)
	TEFF_CUT=$TEFF_CUT_i
	;;
    r)
	TEFF_CUT=$TEFF_CUT_r
	;;
    Y)
	TEFF_CUT=$TEFF_CUT_Y
	;;    
    z)
	TEFF_CUT=$TEFF_CUT_z
	;;
esac
echo "Setting t_eff cut to $TEFF_CUT"
 
echo "figure out overlaps"

# now run the single exposure script to get the overlaps
if [ ! -d mytemp_${EXPNUM} ] ; then
    mkdir mytemp_${EXPNUM}
fi
cd mytemp_${EXPNUM}
ln -s ../exposures_${BAND}.list .
if [ ! -f KH_diff.list2 ] ; then 
    chmod +x ../getOverlaps_single_expo.csh
    ../getOverlaps_single_expo.csh ../exposures_${BAND}.list $ALLEXPS
fi
cd ..

# create the output dag file (empty)
outfile=desgw_pipeline_${EXPNUM}.dag
if [ -f $outfile ]; then
    rm $outfile   # maybe we don't want to overwrite? think about that a bit
fi
touch $outfile

# create the output copy_pairs file (empty)
templatecopyfile="copy_pairs_for_${EXPNUM}.sh"
if [ -f $templatecopyfile ]; then
    rm $templatecopyfile   # maybe we don't want to overwrite? think about that a bit
fi
touch $templatecopyfile

# begin composing the dag 
echo "<parallel>" >> $outfile

# stick a dummy job in here so that there is something just in case there ends up being nothing to do for parallel processing
echo "jobsub -n --group=des --memory=500MB --disk=100MB --expected-lifetime=600s file://dummyjob.sh" >> $outfile


# initialize empty list of files for the copy pairs output
DOTOUTFILES=""

#### make sure all images to be coadded are actually 

# create a search command outfile (to append to the dag after the template jobs, after the upcoming loop)
#AG search.dag --> search_${EXPNUM}.dag
searchfile=search_${EXPNUM}.dag
if [ -f $searchfile ]; then
    rm $searchfile # maybe we don't want to overwrite? think about that a bit
fi
touch $searchfile
echo "<parallel>" >> $searchfile

NOVERLAPS=$(awk '{print NF-2}' mytemp_${EXPNUM}/KH_diff.list1)
# now loop over the diff list, get info about the overlaping exposures, and set the SE portion of the dag
echo "NOVERLAPS $NOVERLAPS"
for((i=1; i<=${NOVERLAPS}; i++)) 
do
    # get expnum, nite info
    echo get expnum, nite info
    overlapnum=$(awk "NR == $i {print \$1}" mytemp_${EXPNUM}/KH_diff.list2)
    overlapnite=$(awk "NR == $i {print \$2}" mytemp_${EXPNUM}/KH_diff.list2)

    # try to use this exposure 
    echo -e "\n try to use exposure ${overlapnum}"
    SKIP=false

    # check that exposure is 30 seconds or longer
    explength=$(awk '($1=='${overlapnum}') {print $7}' exposures.list)
    explength=$(echo $explength | sed -e 's/\.[0-9]*//' )
    if [ $explength -lt 30 ]; then SKIP=true ; fi
    
    # check that exposure's t_eff is greater than the cut for this band
    if [ $i == 1 ]; then
	    echo "this is the search image; dont apply teff cuts"
    else  
	teff=$(awk '($1=='${overlapnum}') {print $10}' exposures.list)
        if [ "${teff}" == "NaN" ]; then
            SKIP=true
            echo "Invalid t_eff value for ${overlapnum}. We will not use this image."
        elif [ $(echo "$teff < $TEFF_CUT" | bc ) -eq 1 ]; then 
            SKIP=true
            echo "Exposure ${overlapnum} has a t_eff of $teff, below the cut value of $TEFF_CUT. We will not use this image."
        fi
    fi
    # check if the exposure is in SE_blacklist.txt                                                                                                                                                       
    for blexp in $blexps
    do
        if [ "$blexp" == "$overlapnum" ] ; then
            SKIP=true
            echo "$overlapnum is in SE_blacklist.txt. Skipping."
        fi
    done

# image failed quality tests ; try the next exposure in the list
    if [ "$SKIP" == "true" ] ; then 
        # we need to remove reference to this exp from the diff.list1 file 
        sed -i -e "s/${overlapnum}//"  mytemp_${EXPNUM}/KH_diff.list1
        # we also need to reduce the count in the first field of KH_diff.list1 by one
        OLDCOUNT=`awk '{print $1}'  mytemp_${EXPNUM}/KH_diff.list1`
        NEWCOUNT=$((${OLDCOUNT}-1))
        sed -i -e s/${OLDCOUNT}/${NEWCOUNT}/  mytemp_${EXPNUM}/KH_diff.list1
        continue
    fi
    
    # the first image in the list is the search image itself
    if [ $i == 1 ]; then 
        if [ "$SKIP" == "true" ] ; then echo "Cannot proceed without the search image!" ; exit 1 ; fi
        NITE=$overlapnite  # capitalized NITE is the search image nite
        SEARCHMJD=$(awk '($1=='${overlapnum}') {print $3}' exposures.list)
    else
        overlapmjd=$(awk '($1=='${overlapnum}') {print $3}' exposures.list)
	if [ $TWINDOW_DISTINCT == 0 ]; then
            # skip if the overlap night is within one of the search exposure night
            if ( [ $(echo "$SEARCHMJD - $overlapmjd < $TWINDOW" | bc ) -eq 1 ] && [ $(echo "$overlapmjd - $SEARCHMJD < $TWINDOW" | bc ) -eq 1 ] )  || [ $overlapnite -lt $MIN_NITE ]  || [ $overlapnite -gt $MAX_NITE ] ; then
		echo "Template $overlapnum is within $TWINDOW MJD of search image, before min night, or after max nite. Skipping this exposure."
		SKIP=true
		# we need to remove reference to this exp from the diff.list1 file
		sed -i -e "s/${overlapnum}//"  mytemp_${EXPNUM}/KH_diff.list1
		# we also need to reduce the count in the first field of KH_diff.list1 by one
		OLDCOUNT=`awk '{print $1}'  mytemp_${EXPNUM}/KH_diff.list1`
		NEWCOUNT=$((${OLDCOUNT}-1))
		sed -i -e s/${OLDCOUNT}/${NEWCOUNT}/  mytemp_${EXPNUM}/KH_diff.list1
		continue
	    fi
	else
	    # skip if the overlap night is within one of the search exposure night
	    if ( [ $(echo "$SEARCHMJD - $overlapmjd < $TWINDOW_BEFORE" | bc ) -eq 1 ] && [ $(echo "$overlapmjd - $SEARCHMJD < $TWINDOW_AFTER" | bc ) -eq 1 ] )  || [ $overlapnite -lt $MIN_NITE ]  || [ $overlapnite -gt $MAX_NITE ] ; then
		echo "Template $overlapnum is within $TWINDOW MJD of search image, before min night, or after max nite. Skipping this exposure."
		SKIP=true
                # we need to remove reference to this exp from the diff.list1 file
		sed -i -e "s/${overlapnum}//"  mytemp_${EXPNUM}/KH_diff.list1
                # we also need to reduce the count in the first field of KH_diff.list1 by one
		OLDCOUNT=`awk '{print $1}'  mytemp_${EXPNUM}/KH_diff.list1`
                NEWCOUNT=$((${OLDCOUNT}-1))
                sed -i -e s/${OLDCOUNT}/${NEWCOUNT}/  mytemp_${EXPNUM}/KH_diff.list1
		continue
	    fi
        fi
    fi
    
    
    #### at this point, the image passed basic quality cuts. let's now check if it was not already SE processed:
    
    echo "overlapnum = ${overlapnum} , overlapnite = ${overlapnite} , explength = $explength, teff = $teff"
    echo checking if image has been SE processed already

    # go ahead and run copy_DESDM.sh anyway. If it detects everything is already there, then it will just return quickly.
    echo $overlapnum > tempoverlap.list
#    ./copy_DESDM.sh tempoverlap.list
    rm tempoverlap.list

    # ls in the dcache scratch area to see if images are already there
    nfiles=0    
    for file in `ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/*_${rpnum}_immask.fits.fz`
    do
        if [ `stat -c %s $file` -gt 0 ]; then nfiles=`expr $nfiles + 1` ; fi	
    done
    
    # ls in the dcache scratch area to see if sextractor files are already there
    mfiles=0    
    for file in `ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/*_${rpnum}_fullcat.fits*`
    do
        if [ `stat -c %s $file` -gt 0 ]; then mfiles=`expr $mfiles + 1` ;  fi	
    done
    
    
    # check the .out file too
    if [ -e /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/${overlapnum}.out ]; then
        ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/${overlapnum}.out 
    else
	# if all the fits files are there, try to produce the missing .out file quickly
        if [ $nfiles -ge 59 ] ; then
            echo "************* doing getcorners *****************"
            ./getcorners.sh $overlapnum /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum} .
            ifdh cp -D ${overlapnum}.out /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}
            if [ $? -ne 0 ] ; then 
                echo "Warning: Missing .out file: /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/${overlapnum}.out" 
                # assume something went wrong with the previous SE proc for this image. set nfiles=0 to force reprocessing
                nfiles=0
            fi
        fi
    fi
    # check if we need to run the calibration. If we don't have SNSTAR and SNVETO defined in the .INPUT file, then assume we don't
    # we want to do calibration, and the SNSTAR and SNVETO names are defined
    if  [[ $SEARCH_OPTS == *-C* ]] && ( [ ! -z "${SNSTAR_FILENAME}" ] || [ ! -z "${SNVETO_FILENAME}" ] ) ; then
        # check if calibration outputs are present
        # if number of reduced images and sextractor catalogs is not the same, something looks fishy. set nfiles=0 to force reprocessing 
        if [ $mfiles -ne $nfiles ] ; then nfiles=0 ; fi 
        JUMPTOEXPCALIBOPTION=""
        if [ -e /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/allZP_D`printf %08d ${overlapnum}`_${rpnum}.csv ] && [ -e /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/D`printf %08d ${overlapnum}`_${rpnum}_ZP.csv ]; then
            ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/allZP_D`printf %08d ${overlapnum}`_${rpnum}.csv 
        else
            # if only the expCalib outputs are missing and we are not allowed to ignore them
	    ncsv=$(ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${overlapnite}/${overlapnum}/D`printf %08d ${overlapnum}`_[0-6][0-9]_${rpnum}_ZP.csv | wc -w 2>/dev/null) 
            if [ $ncsv -lt 59 ] && [ $nfiles -ge 59 ] && [ "$IGNORECALIB" == "true" ] ; then
                # assume something went wrong with the previous SE proc for this image (set nfiles=0 to force reprocessing)
                nfiles=0
                # but assume that only calibration step needs to be done for this exposure
                JUMPTOEXPCALIBOPTION="-j"
                echo "Warning: Missing outputs of expCalib. Will jump directly to the calibration step for this image."
            fi
        fi
    fi
    # if there are 59+ files with non-zero size, a .out file, and expCalib outputs, then don't do the SE job again for that exposure     
    if [ $nfiles -ge 59 ] ; then
        echo "SE proc. already complete for exposure $overlapnum"
	# since we go into the next if statement for the search (first) image anyway, only add the .out file here
	# for i > 1; otherwise it will appear twice.
        if [ $i -gt 1 ]; then 
            DOTOUTFILES="${DOTOUTFILES} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/${overlapnum}.out"
        fi
    fi
    if [ $nfiles -lt 59 ] || [ $i == 1 ]; then
	    if [ $i == 1 ]; then echo "This is the search image so we need to make sure that the raw image is stil present." ; fi
	
                # make sure that the directory for the raw image exists and has the appropriate permissions
        if [ ! -d /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/ ]; then
            mkdir /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/
            chmod 775  /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/
        fi
	
        if [ "${SKIP_INCOMPLETE_SE}" == "false" ] || [ $i == 1 ]; then
            if [ -e /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ] && [ -e /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ]; then
                cmp --silent /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz || { $COPYCMD /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ; }
            fi
                    # check if the raw image is present so that the SE processing can run. If it isn't, try to pull it over from des51.b or NCSA DESDM	
            if [ -e /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ]; then
                echo "Raw image for exposure $overlapnum present in dCache"
            elif [ -e /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz ]; then
                echo "Raw image for exposure $overlapnum not present in dCache; trying from des51.b"
                $COPYCMD /data/des51.b/data/DTS/src/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz || { echo "cp failed!" ; exit 2 ; }
            else 
                #echo " Raw image for exposure $overlapnum not present in dcache or /data/des51.b. Try to transfer from NCSA..."
                #export WGETRC=$HOME/.wgetrc-desdm
                #if [ ! -f $WGETRC ] ; then echo "Warning: Missing file $HOME/.wgetrc-desdm may cause wget authentication error." ; fi
                #wget --no-check-certificate -nv https://desar2.cosmology.illinois.edu/DESFiles/desarchive/DTS/raw/${overlapnite}/DECam_`printf %08d ${overlapnum}`.fits.fz 
                #if [ $? -eq 0 ] ; then
                #    $COPYDCMD DECam_`printf %08d ${overlapnum}`.fits.fz /pnfs/des/scratch/${SCHEMA}/dts/${overlapnite}/ && rm DECam_`printf %08d ${overlapnum}`.fits.fz
                #else
                    echo "wget failed!  skip this exposure" #"wget failed! Will try to get image $overlapnum $overlapnite from NOAO."			
                            #fetch_noao
                            #if [ $? -ne 0 ]; then
                            #echo "Failure in fetching from NOAO!"
                            #if [ $i == 1 ] ; then echo "Cannot proceed without the search image!" ; exit 2 ; fi
                    SKIP=true
                            #echo "Unable to find raw image for overlapping exposure: $overlapnum ; will try to proceed without it."
                    
                            ###### remove the overlap from the diff.list file
                    sed -i -e "s/${overlapnum}//"  mytemp_${EXPNUM}/KH_diff.list1
                            # we also need to reduce the count in the first field of KH_diff.list1 by one
                    OLDCOUNT=`awk '{print $1}'  mytemp_${EXPNUM}/KH_diff.list1`
                    NEWCOUNT=$((${OLDCOUNT}-1))
                    sed -i -e s/" ${OLDCOUNT} "/" ${NEWCOUNT} "/  mytemp_${EXPNUM}/KH_diff.list1 
                #fi # if [ $? -eq 0 ]
            fi
        fi #incomplete SE
        #else # this is not the search image so we don't check for the raw image
        #    continue
	
    fi # nfile -ge 59
    
    
    if [ ! -z "$SNSTAR_FILENAME" ]; then
        SNSTAR_OPTS="-T $SNSTAR_FILENAME"
    fi
    if [ ! -z "$SNVETO_FILENAME" ]; then
        SNVETO_OPTS="-V $SNVETO_FILENAME" 
    fi
    
    # add the SE+diff jobs to the dag
    echo add the SE+diff jobs to the dag

    
    if [ $nfiles -lt 59 ] || [ $i -eq 1 ]; then   
        if [ "${SKIP}" == "true" ] && [ $i -ne 1 ]; # skip incomplete SE, but only for templates, if getting the missing templates failed
        then
            SKIP=true
            ###### remove the overlap from the diff.list file
            echo "removing overlap $overlapnum from diff.list file"
            sed -i -e "s/${overlapnum}//"  mytemp_${EXPNUM}/KH_diff.list1
            # we also need to reduce the count in the first field of KH_diff.list1 by one
            OLDCOUNT=`awk '{print $1}'  mytemp_${EXPNUM}/KH_diff.list1`
            NEWCOUNT=$((${OLDCOUNT}-1))
            sed -i -e s/" ${OLDCOUNT} "/" ${NEWCOUNT} "/  mytemp_${EXPNUM}/KH_diff.list1 
        else
            EXPLIST=$(echo ${ALLEXPS// /,})
            if [ "$overlapnum" == "$EXPNUM" ]; then
                # search image (no -t option)
                # write to a different text file, then append that at the end (to ensure templates are done before the search)
                for (( ichip=1;ichip<63;ichip++ ))
                do
                    if [ $ichip -ne 2 ] && [ $ichip -ne 31 ] && [ $ichip -ne 61 ] ; then
			# CHANGE 11-20-18 TO RUN SEDiff ON ALL EXPOSURES EXPLIST --> overlapnum
                        echo "jobsub -n --group=des --singularity-image /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-wn-sl7:latest $JOBSUB_OPTS --append_condor_requirements='(TARGET.GLIDEIN_Site==\\\"FermiGrid\\\"||(TARGET.HAS_SINGULARITY==true&&TARGET.HAS_CVMFS_des_opensciencegrid_org==true&&TARGET.HAS_CVMFS_des_osgstorage_org==true)${STASHVER})' file://SEdiff.sh -r $RNUM -p $PNUM -E $EXPLIST -v $DIFFIMG_EUPS_VERSION -b $BAND -n $overlapnite $JUMPTOEXPCALIBOPTION -d $DESTCACHE -m $SCHEMA $SEARCH_OPTS -c $ichip -S $procnum $(echo $SNSTAR_OPTS | sed -e "s/\${CCDNUM_LIST}/${ichip}/") $(echo $SNVETO_OPTS | sed -e "s/\${CCDNUM_LIST}/${ichip}/")" >> $searchfile
    #                       echo wrote chip $ichip to $searchfile
                    fi    
                done
#                cat $searchfile
            else		
		echo "jobsub -n --group=des --singularity-image /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-wn-sl7:latest $JOBSUB_OPTS -N 60 --append_condor_requirements='(TARGET.GLIDEIN_Site==\\\"FermiGrid\\\"||(TARGET.HAS_SINGULARITY==true&&TARGET.HAS_CVMFS_des_opensciencegrid_org==true&&TARGET.HAS_CVMFS_des_osgstorage_org==true))' file://SEdiff.sh -r $RNUM -p $PNUM -E $overlapnum -v $DIFFIMG_EUPS_VERSION -b $BAND -n $overlapnite $JUMPTOEXPCALIBOPTION -d $DESTCACHE -m $SCHEMA -c 0 -t $TEMP_OPTS -S $procnum" >> $outfile
            fi
            # add the .out file for this overlap image to the list to be copied
            DOTOUTFILES="${DOTOUTFILES} /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/$overlapnite/$overlapnum/${overlapnum}.out"
        fi
    fi
    echo
done
# close the template portion of the dag
echo "</parallel>" >> $outfile

# close the search portion of the dag
echo "</parallel>" >> $searchfile
cat $searchfile >> $outfile

# write the full copy command for the .out files and other auxfiles
echo "ifdh cp -D $DOTOUTFILES \$TOPDIR_WSTEMPLATES/pairs/" > $templatecopyfile

# check if there are templates or if diffimg will fail
if [ $(awk '{print $1}' mytemp_${EXPNUM}/KH_diff.list1 ) -le 1 ]; then
    echo "There appear to be no templates for this exposure $EXPNUM. Diffimg will fail."
    NOTEMPS=1
else
    NOTEMPS=0
fi

export DES_SERVICES=~/.desservices.ini
export DES_DB_SECTION=db-sn-test
export SCAMP_CATALOG_DIR=/cvmfs/des.opensciencegrid.org/fnal/SNscampCatalog
export AUTOSCAN_PYTHON=$PYTHON_DIR/bin/python

#this gets the fake DB version file
### make sure that this is still there!!! if not check des41
export DES_ROOT=/data/des20.b/data/SNDATA_ROOT/INTERNAL/DES

export TOPDIR_SNFORCEPHOTO_IMAGES=data/DESSN_PIPELINE/SNFORCE/IMAGES
export TOPDIR_SNFORCEPHOTO_OUTPUT=data/DESSN_PIPELINE/SNFORCE/OUTPUT
export TOPDIR_DATAFILES_PUBLIC=data/DESSN_PIPELINE/SNFORCE/DATAFILES_TEST
export TOPDIR_TEMPLATES=/data/des30.a/data/WSTemplates

#FIELD_TILING=$(gethead /pnfs/des/scratch/${SCHEMA}/dts/${NITE}/DECam_`printf %08d ${EXPNUM}`.fits.fz OBJECT | sed -r -e 's/.*DES.*hex\s*//' -e 's/tiling//' -e s/\"// )

#echo "FIELD_TILING = $FIELD_TILING"
#echo "Modifying FIELD, OBJECT, and TILING to match schema"

############ check and do something special if the word hex is not present ############

imageline=$(awk '($1=='${EXPNUM}') {print $4,$5}' exposures_${BAND}.list)
SEARCHRA=`echo $imageline | cut -d " " -f 1`
SEARCHDEC=`echo $imageline | cut -d " " -f 2`
RA10=$(echo "${SEARCHRA}*10" | bc | cut -d "." -f 1)
if [ -z "$RA10" ] ; then RA10=0 ; fi
DEC10=$(echo "$SEARCHDEC * 10" | bc | cut -d "." -f 1)
if [ -z "$DEC10" ] ; then DEC10=0 ; fi
if [ $DEC10 -ge 0 ]; then 
    DEC10="+${DEC10}"
fi
FIELD="WS${RA10}${DEC10}"
TILING=1
FIELD_TILING="${FIELD} ${TILING}"
if [ $DO_HEADER_CHECK -eq 1 ]; then
    check_header
fi

if [ $(awk '{print $1}' mytemp_${EXPNUM}/KH_diff.list1 ) -gt 1 ]; then
# Add the runmon step at the end of the dag. wait until now because we need to determine the field first. Only do it if there are templates though, since there will be no diffimg jobs otherwise.
echo "<serial>" >> $outfile
echo "jobsub -n --group=des --singularity-image /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-wn-sl7:latest $JOBSUB_OPTS --expected-lifetime=7200s  --append_condor_requirements='(TARGET.GLIDEIN_Site==\\\"FermiGrid\\\"||TARGET.HAS_CVMFS_des_opensciencegrid_org==true)' file://RUNMON.sh -r $rpnum -p $procnum -E $EXPNUM -n $NITE -f $FIELD -d $DESTCACHE -m $SCHEMA" >> $outfile
echo "</serial>" >> $outfile
fi
# edit the template files to match this exposure
#REALCCD='$(eval "echo $SNSTAR_FILENAME")'
# this is written to MAKE_DIFFIMG_DIRS_${EXPNUM}.INPUT, where the variable is used to create the RUN02 makeStarcat options
sed -e s/THENITE/$NITE/ -e s/THEBAND/${BAND}/ -e s/THEEXP/${EXPNUM}/ -e s/THEFIELD/${FIELD}/ -e s/THEPROCNUM/${procnum}/ -e s/THESEASON/${SEASON}/ -e s/THERNUM/${RNUM}/ -e s/THEPNUM/${PNUM}/ -e s/THECCDNUM/'\${CCDNUM_LIST}'/ MAKESCRIPT_DIFFIMG_TEMPLATE.INPUT > MAKE_DIFFIMG_DIRS_${EXPNUM}.INPUT

sed -e s/THENITE/$NITE/ -e s/THEBAND/${BAND}/ -e s/THEEXP/${EXPNUM}/ -e s/THEFIELD/${FIELD}/ -e s/THETILE/${TILING}/ -e s/CCD2DIGIT/\$CCD/ -e "s/ALLEXP/${ALLEXPS}/" INTERNAL_INFO_TEMPLATE.DAT > INTERNAL_INFO_${EXPNUM}_tile${TILING}.DAT

# create dir for this exposure, and put relevant files there

mkdir -p mytemp_${EXPNUM}/${procnum}
mkdir -p mytemp_${EXPNUM}/${procnum}/input_files/
chmod -R g+w mytemp_${EXPNUM}

cd mytemp_${EXPNUM}

if [ -e JOBDIR ]; then rm JOBDIR ; fi
if [ -e mytemp_${EXPNUM} ]; then 
    rm mytemp_${EXPNUM}
    ln -s . mytemp_${EXPNUM}
fi
ln -s . JOBDIR

mv ../INTERNAL_INFO_${EXPNUM}_tile${TILING}.DAT ./INTERNAL_INFO_${EXPNUM}_tile${TILING}.KH

lisfile=WSinput_${NITE}_${FIELD}.lis
if [ -f $lisfile ]; then 
rm $lisfile
fi
touch $lisfile

for myexp in $ALLEXPS
do
echo "ROOT: /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${myexp}" >> $lisfile
echo "FILE: $FIELD $BAND $TILING $myexp D$(printf "%08d" ${myexp})_${BAND}_\$CCD_${rpnum}_immask.fits " >> $lisfile
done

OPWD=$PWD

mv ../MAKE_DIFFIMG_DIRS_${EXPNUM}.INPUT .

#writeDB on/off
if [ "$WRITEDB" == "on" ] ; then 
    RUN_DIFFIMG_PIPELINE.pl  MAKE_DIFFIMG_DIRS_${EXPNUM}.INPUT NOPROMPT -writeDB
else
    RUN_DIFFIMG_PIPELINE.pl  MAKE_DIFFIMG_DIRS_${EXPNUM}.INPUT NOPROMPT
fi

for ((iccd=1;iccd<=62;iccd++))
do
    if [ $iccd -ne 2 ] && [ $iccd -ne 31 ] && [ $iccd -ne 61 ]; then

	sed -i -e '/ln -sf/ s/ln -sf/ifdh cp/' -e "s/fits/fits.fz/g" ./${procnum}/${BAND}_`printf %02d ${iccd}`/RUN01_expose_prepData
	newlink="$(egrep "^ifdh"  ./${procnum}/${BAND}_`printf %02d ${iccd}`/RUN01_expose_prepData | awk '{print $4}')"
	echo "newlink = $newlink"
	for mylink in $newlink
	do
	    nicelink=$(echo $mylink | sed -e 's/\//\\\//g')
	    sed -i -e '/ifdh cp\s.*\s'$nicelink'/a funpack -D '$mylink'\n ln -sf '$mylink' JOBDIR' ./${procnum}/${BAND}_`printf %02d ${iccd}`/RUN01_expose_prepData
	    sed -i -e '/ln -sf/ s/.fz//' ./${procnum}/${BAND}_`printf %02d ${iccd}`/RUN01_expose_prepData
	done

	#move the funpack -D and ln -sf lines to after the if statement in RUN01
        #assuming its always written the same, the first funpack is on line 6, the rest 9 lines after. 
        #For the funpack line to go after the if statement, line 6 moves down 6 lines
        # ie. lines 6,7 --> 12,13, lines 15,16 --> 21,22, etc
	length=`cat ./${procnum}/${BAND}_\`printf %02d ${iccd}\`/RUN01_expose_prepData | wc -l`
	for ((i=6;i<=$length;i=i+9));
	do
	    m=$(($i+1))
            n=$(($i+6))
            l=$(($i+7))
            if [ $m -le $length ] || [ $n -le $length ] || [ $l -le $length ]; then
		ex -s -c ${i},${m}m${n},${l} -c w -c q ./${procnum}/${BAND}_`printf %02d ${iccd}`/RUN01_expose_prepData
            fi
	done

	# Now edit the RUN22 script if we're using a veto catalog from a file

	if [ ! -z "${SNVETO_FILENAME}" ] ; then
	    sed -i -e '/inFile_param/ a\  -inFile_veto       '$SNVETO_FILENAME' \\'  ./${procnum}/${BAND}_`printf %02d ${iccd}`/RUN22_combined+expose_filterObj    
	fi
    fi
done


cd ../

mv $templatecopyfile  mytemp_${EXPNUM}/${procnum}/input_files/

files2copy=$(ifdh ls mytemp_${EXPNUM}/${procnum}/ )
outlist=""
dirlist=""
for file in $files2copy
do
    case $file in
	*//)
	    :
	    ;;
	*/)
	    dirlist="$dirlist $file"
	    ;;
	*)
# do not copy zero-length files
	    if [ `stat -c %s $file` -gt 0 ]; then
		outlist="$outlist $file"
	    else
		echo "File $file has zero length; skipping copy"
	    fi
	    ;;
    esac
done

if [ "${USER}" == "desgw" ] ; then
    MKDIRCMD="mkdir -p"
    CHMODCMD="chmod g+w"
else
    MKDIRCMD="ifdh mkdir_p"
    CHMODCMD="ifdh chmod 775"
fi


if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/ ]; then
    echo "Creating output directory for search night"
    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/
    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/
fi

if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM} ]; then
    echo "Creating output directory for search image"
    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}
    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}
fi
if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum} ]; then
    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}
    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}
fi
if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE} ]; then
    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE}
    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/forcephoto/images/${procnum}/${NITE} 
fi

for (( ichip=1;ichip<63;ichip++))
do
    if [ $ichip -ne 2 ] && [ $ichip -ne 31 ] && [ $ichip -ne 61 ]; then
	
	if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip` ]; then
	    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`
	    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`
	fi
	if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/ingest ]; then
	    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/ingest
	    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/ingest
	fi
	if [ ! -d /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/stamps_${NITE}_${FIELD}_${BAND}_`printf %02d $ichip` ]; then
	    $MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/stamps_${NITE}_${FIELD}_${BAND}_`printf %02d $ichip`
	    $CHMODCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/stamps_${NITE}_${FIELD}_${BAND}_`printf %02d $ichip`
	fi
    fi
done

PREVDIR=$PWD
cd mytemp_${EXPNUM}
tar czf  ${EXPNUM}_run_inputs.tar.gz ${procnum}/${BAND}_[0-9][0-9] ${procnum}/SN_mon*.list ${procnum}/FILTERCHIP* ${procnum}/PROCFILES.LIST *.lis ${procnum}/input_files/
cd $PREVDIR

rm -f  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${BAND}_`printf %02d $ichip`/${EXPNUM}_${BAND}_`printf %02d ${ichip}`_run_inputs.tar.gz  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/${EXPNUM}_run_inputs.tar.gz

rm -rf /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files

### modify VETOMAG in SN_cuts.filterObj ##### 
sed -i -e '/VETOMAG/ s/21/20/'  mytemp_${EXPNUM}/${procnum}/input_files/SN_cuts.filterObj
#sed -i -e '/MIN_MLSCORE/ s/0.3/0.25/'  mytemp_${EXPNUM}/${procnum}/input_files/SN_cuts.filterObj

echo "now doing coy of input_files directory"
echo "copydcmd = $COPYDCMD"
echo "ls first:"
ls /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/ 
#if [ ${USER} != 'desgw' ]; then
$MKDIRCMD /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/input_files
#fi  
$COPYDCMD -r mytemp_${EXPNUM}/${procnum}/input_files /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/  ||  echo "Error copying data dir!!!!"

rmlist=""
for file in $outlist
do
rmlist="$rmlist /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/`basename $file`"
done

rm -rf $rmlist
$COPYDCMD $outlist mytemp_${EXPNUM}/${EXPNUM}_run_inputs.tar.gz /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/${procnum}/ ||  echo "Error copying input files to dCache!!!!"
#ifdh cp -r -D mytemp_${EXPNUM}/data /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/ ||  echo "Error copying data dir!!!!"

#ifdh rename 
#if [ "${USER}" == "desgw" ]; then
#    COPYCMD="cp"
#else
#    COPYCMD="ifdh cp"
#fi
rm -f /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list
$COPYCMD mytemp_${EXPNUM}/KH_diff.list1  /pnfs/des/${DESTCACHE}/${SCHEMA}/exp/${NITE}/${EXPNUM}/WS_diff.list

if [ $NOTEMPS -eq 1 ]; then
    echo "NO TEMPLATE IMAGES, DIFFIMG WILL FAIL"
fi

echo "To submit this DAG do"
echo "jobsub_submit_dag -G des --role=desgw --need-storage-modify /des/persistent/gw/exp --need-storage-modify /des/persistent/gw/forcephoto file://${outfile}"

touch mytemp_${EXPNUM}/DAGMaker.DONE
echo "jobsub_submit_dag -G des --role=desgw --need-storage-modify /des/persistent/gw/exp --need-storage-modify /des/persistent/gw/forcephoto file://${outfile}" >> mytemp_${EXPNUM}/DAGMaker.DONE
rm -r syspfiles_$$
