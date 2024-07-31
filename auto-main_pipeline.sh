#!/bin/bash

if [ $# -lt 1 ]; then
    echo "usage: auto-main.sh -S SEAON -E EXP_NUMS [-r RNUM] [-p PNUM] [-v EUPS_VERSION] [-L LIGOID] [-T TRIGGERID] [-M TRIGGER_MJD] [-h HELP]"
    exit 1
fi

##############
# PARSE ARGS #
##############

RNUM=4
PNUM=12
EUPS_VERSION="gw8"
export LIGOID="None"
export TRIGGERID="None"
export TRIGGERMJD="None"

#since EXP_NUMS is a space-separated list, args must be parsed in this way
while getopts "S:E:r:p:v:L:T:M:h" opt "$@" ;
do case $opt in
    S)
        export SEASON=$OPTARG
        ;;
    E)
        export EXP_NUMS=$OPTARG
        ;;
    r)
        export RNUM=$OPTARG
        ;;
    p)
        export PNUM=$OPTARG
        ;;
    v)
        export EUPS_VERSION=$OPTARG
        ;;
    L)
        export LIGOID=$OPTARG
        ;;
    P)
        export PROPID=$OPTARG
        ;;
    T)
        export TRIGGERID=$OPTARG
        ;;
    M)
        export TRIGGERMJD=$OPTARG
        ;;
    h)
        echo "usage: auto-main.sh -S SEAON -E EXP_NUMS [-r RNUM] [-p PNUM] [-v EUPS_VERSION] [-L LIGOID] [-T TRIGGERID] [-M TRIGGER_MJD] [-h HELP]"
        exit 1
        ;;
esac
done

#################
# SE PROCESSING #
#################

echo "Beginning SE setup"
#make directory for SE processing
if ! test -d ./SE_proc ; then
    mkdir ./SE_proc
    cp -p ./auto-write_DAGMaker.sh ./SE_proc/auto-write_DAGMaker.sh
fi

#get essential SE processing information
    #needed for postprocessing
    #sorts exposures into lists by band, returns a list of band lists
eval `python ./auto-SE_prep.py --season $SEASON --expnums "$EXP_NUMS"`

cd ./SE_proc
#write the dagmaker for the season
if ! test -f ./dagmaker.rc ; then
    . ./auto-write_DAGMaker.sh
fi

#run DAGmaker for each exposure
    #exposures are sorted by band
    #each band is run in parallel
    #exposures within band are run in series
        #this is to avoid accidentally cowriting to the same template images
echo "Beginning SE processing"
for band_list in "${BANDS[@]}" ; do (
    #this declaration is necessary to loop over array of arrays
    band="$band_list[@]"
    for exposure in "${!band}" ; do
        #make workspace directory for each exposure
        if ! test -d ./${exposure} ; then
            mkdir ./${exposure}
            cp -rp ../auto-SEproc_scripts/gw-workflow/. ./${exposure}/
            cp -p ./dagmaker.rc ./${exposure}
        fi
        cd ./${exposure}

        #run DAGMaker for each exposure if needed
            #run as exec because DAG job must be submitted outside of container
        if ! test -f ./desgw_pipeline_${exposure}.dag ; then
            echo "running DAGMaker for ${exposure}"
            /cvmfs/oasis.opensciencegrid.org/mis/apptainer/current/bin/apptainer exec -B /cvmfs,/home,/data/des90.a,/data/des90.b,/data/des80.a,/data/des80.b,/data/des70.a,/data/des70.b,/data/des91.a,/data/des91.b,/data/des81.a,/data/des81.b,/data/des71.a,/data/des71.b,/data/des61.a,/data/des61.b,/data/des60.a,/data/des60.b,/data/des51.a,/data/des51.b,/data/des50.a,/data/des50.b,/data/des40.a,/data/des40.b,/data/des41.a,/data/des41.b,/pnfs/des,/opt,/run/user,/etc/hostname,/etc/hosts,/etc/krb5.conf --ipc --pid /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-dev-sl7:latest ./DAGMaker.sh $exposure &> ./dag_out.out
        fi

        #submit DAG job for each exposure
            #must be run on des70
        if ! test -f ./jobsub_${exposure}.out ; then
            echo "submitting DAG job for ${exposure}"
            jobsub_submit_dag -G des --role=desgw --need-storage-modify /des/persistent/gw/exp --need-storage-modify /des/persistent/gw/forcephoto file://desgw_pipeline_${exposure}.dag &> jobsub_${exposure}.out
        fi

        echo "SE processing complete for ${exposure}"
        cd ..
    done
    echo "SE processing complete for ${band_list}"
) &
done
wait
echo "SE processing complete"
cd ..

##################
# POSTPROCESSING #
##################

echo "Beginning PostProc setup"
if ! test -d ./PostProc ; then
    mkdir ./PostProc
    cp -rp ./auto-PostProc_scripts/Post-Processing/. ./PostProc/
    cp -p ./auto-PostProc.py ./PostProc/auto-PostProc.py
fi
#this line is temporary
cp -p ./auto-PostProc.py ./PostProc/auto-PostProc.py

cd ./PostProc

echo "Beginning PostProc"
/cvmfs/oasis.opensciencegrid.org/mis/apptainer/current/bin/apptainer exec -B /cvmfs,/home,/data/des90.a,/data/des90.b,/data/des80.a,/data/des80.b,/data/des70.a,/data/des70.b,/data/des91.a,/data/des91.b,/data/des81.a,/data/des81.b,/data/des71.a,/data/des71.b,/data/des61.a,/data/des61.b,/data/des60.a,/data/des60.b,/data/des51.a,/data/des51.b,/data/des50.a,/data/des50.b,/data/des40.a,/data/des40.b,/data/des41.a,/data/des41.b,/pnfs/des,/opt,/run/user,/etc/hostname,/etc/hosts,/etc/krb5.conf --ipc --pid /cvmfs/singularity.opensciencegrid.org/fermilab/fnal-dev-sl7:latest python ./auto-PostProc.py --ligoid $LIGOID --triggerid $TRIGGERID --triggermjd $TRIGGERMJD &> ./postproc_out.out
echo "PostProc complete"