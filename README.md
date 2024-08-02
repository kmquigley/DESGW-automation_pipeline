# DESGW-automation_pipeline

    The goal of this pipeline is to string together the various scripts needed for GW optical follow-up (DAGMaker, SE processing, Post-Processing) so that it runs as a single package with no interference needed.

## CONTENTS:
    
    The only files and directories needed to run the pipeline are those prefixed with "auto-".  These are:
        ./auto-main.sh
        ./auto-main_pipeline.sh
        ./auto-SE_prep.py
        ./auto-exposures.list
        ./auto-write_DAGMaker.sh
        ./auto-SEproc_scripts
        ./auto-PostProc_scripts
        ./auto-PostProc_setup.sh
        ./auto-PostProc.py
    Other directories will be created throughout the course of the pipeline.  These are:
        ./SE_proc
        ./SE_output
        ./Post-Processing
        ./Post-Processing_output

## RUNNING THE PIPELINE:
    
    Must be logged in as the desgw user on des70.fnal.gov
    Must specify variables in ./auto-main.sh:
        HOME_DIR    where the pipeline will be run (this is the automation_pipeline directory path)
        SEASON      season number of the analysis
        EXP_NUMS    space-separated list of exposure numbers to be processed, passed in single quotation marks
    
    ************************
    * When user is logged in and variables are defined, all that needs to be done is to run 
    *
    *    auto-main.sh
    *    
    * in the terminal from any location.  No arguments are required.  
    * All other necessary containers, scripts, and packages will be automatically imported and set up.
    ************************

    Alternatively, ./auto-main_pipeline.sh may be run directly in the automation-pipeline directory with SEASON and EXP_NUMS passed as arguments.
    An example of this option is given at the end of ./auto-main.sh

## DOCUMENTATION AND NOTES:
    
### ./auto-main.sh

    This is the only file that needs to be run for the pipeline.  
        It can be run from any location, as it will cd to the HOME_DIR
        User must be logged in as desgw on the des70 machine for the pipeline to work.
    
    Defines variables and runs ./auto-main_pipeline.sh

    Here is where HOME_DIR, SEASON, EXP_NUMS, and other arguments may be defined.
    Contains an example of how to log in as the desgw user on des70.fnal.gov
    Contains an example of how to run ./auto-main_pipeline.sh directly.
        
### ./auto-main_pipeline.sh

    This is the main script of the pipeline.
    Parses arguments, runs single exposure processing, and runs post-processing.

    Required Arguments:
        -S SEASON           Season number of the analysis.
        -E EXP_NUMS         Space-separated list of exposure numbers to be processed, passed in single quotation marks.
    
    Optional Arguments:
        -r RNUM             Default is 4.
        -p PNUM             Default is 12.
        -v EUPS_VERSION     Default is "gw8".
        -L LIGOID           Ligo ID for the GW event.  Default is "None". 
                            ** NOT defined during the course of the pipeline.
        -T TRIGGERID        Trigger ID for the GW event.  Default is "None".  
                            ** NOT defined during the course of the pipeline.
        -M TRIGGERMJD       Trigger Modified Julian Date for the GW event.  Default is "None".  
                            ** NOT defined during the course of the pipeline.
        -h HELP             Describes the other function arguments.

#### SE (Single-Exposure) Processing:

    Creates ./SE_proc directory if it doesn't already exist.
    Runs ./auto-SE_prep.py
        Reads ./auto-exposures.list
            This is identical to the regular exposures.list
            *** This file is NOT found in the git repository.  It must be copied in from somewhere else and renamed to "./auto-exposures.list"
        Collects BAND, SEASON, and PROPID for each exposure in EXP_NUMS
        Records this information in ./SE_output/output.txt, which will be created if it doesn't already exist.
            This will be read by ./auto-PostProc.py
        Exposures are sorted into bands, and a list of bands is created.
            This is so that SE processing can be partially parallelized.
    Runs ./auto-write-DAGMaker.sh within ./SE_proc
        Creates the dagmaker.rc for the season within ./SE_proc
    
    Each band (u, g, r, i, z, y, etc) is run in parallel:
        Exposures within each band are run in series:
            This is to avoid writing to the same template image multiple times at once and corrupting the data in dCache

            A directory is created within ./SE_proc for each exposure, if it doesn't already exist.
            This exposure directory contains the essential contents of the gw-workflow folder and the dagmaker.rc for the season.

            DAGMaker.sh is run within the exposure directory if it has not been run already.
                ** This is NOT identical to gw-workflow/DAGMaker.sh
                    Edits made:
                        If template image in dCache doesn't match des51.b, the des51.b version is copied into dCache.
                            This is to remedy files which have been corrupted or interrupted during writing.
                        If template images are not available in dCache, the only backup source is in des51.b because:
                            des30.b is now depreciated.
                            des41.b is not a general fix.
(!)                     *** The NCSA fetch currently returns a 403: FORBIDDEN error! ***
                            The NOAO fetch was never implemented on the original DAGMaker.sh
                        If template images are still incomplete after checking des51.b, they are skipped.
                            Line 694, "${SKIP_INCOMPLETE_SE}" == "false" changed to "${SKIP}" == "true"
                            This is to avoid "fits file not found" errors which greatly slow down SEdiff jobs.
                        The copy command is always "idfh cp", instead of switching to "cp" for the desgw user.
                            "cp" does not seem to correctly copy des51.b shortcuts into dCache.
                DAGMaker.sh is run within a container.
                    The full mount list actually is required for template image retrieval.
                Produces "dag_out.out" log.
                Produces a "desgw_pipeline dag" job for the exposure.
            
            DAG job is submitted from within the exposure directory if it has not been submitted already.
                DAG job must be submitted outside of the container for security reasons.
                Uses jobsub client, which is only available on des70.fnal.gov
                The underlying file that the job runs is SEdiff.sh
                    ** This is NOT identical to gw-workflow/SEdiff.sh
                        Edits made:
                            ./expCalib-isaac-BBH.py and ./expCalib-isaac-BNS.py are copied into the job folder earlier to avoid "file not found" errors
(!)                     *** Other small bigfixes are also needed in this file! ***
                Produces a "jobsub out" log for the exposure.
                    The bottom of this log gives a "########.0@jobsub0#.fnal.gov" location.
                    This location can be used to check on the status of the job at
                        https://fifemon.fnal.gov/monitor/d/000000115/job-cluster-summary 
    
(!) *** In the future it would be wise to put a wait timer (how long?) between SE processing and Post-Processing! ***

#### Post-Processing:
    Creates ./Post-Processing and ./Post-Processing_output directories if they don't already exist.
    This Post-Processing directory contains the essential contents of the Post-Processing folder and ./auto-PostProc.py
    Runs ./auto-PostProc_setup within ./Post-Processing inside a container.
        Sets up needed packages for Post-Processing.
        Runs ./auto-PostProc.py
            ** This is adapted from but NOT identical to DESGW-pipeline_automation/postprocessing_automations.py
                Edits made:
                    Several simplifications and minor fixes.
                    Removal of git pull of Post-Processing folder.
                        These files are already included within the pipeline.
                    Removal of user prompting for ID and exposure numbers
                        These should be passed in already from the beginning of the pipeline.
                    configParser package removed; the work it did has now been coded by hand.
                        The loaded version is for Python 2.7 and does not run correctly.
            Takes in arguments LIGOID, TRIGGERID, TRIGGERMJD from ./auto-main_pipeline.sh
                SEASON, EXPOSURES, and PROPID are read from ./SE_output
            Detects whether SE processing was successful for each CCD, or where it failed.
            Creates a completed exposure list and a "postproc ini" file for the season.
            If there are enough completed CCDs and exposures to move on, it continues with Post-Processing.
            Runs diffimg_setup.sh
            Runs run_postproc.py, which in turn runs postproc.py
                This is the actual Post-Processing process.

## CURRENT STATUS:
    
    Pipeline is completely successful at SE Processing setup and submitting SE Processing jobs in parallel.
    Pipeline is completely successful at Post-Processing setup.
    The pipeline has mostly been tested on the newest and oldest GW events.
        These exposures tend to have template images which are missing from dCache and des51.b
        This has led to a large number of "fits file not found" or "failed to read fits" errors.
            Ultimately these lead to new errors involving "xtalk" files.
    Other current hangups have made it difficult to get a SE Processing success.
    Because there have been few successful SE Processing jobs, the pipeline is not well-tested on the Post-Processing section.

    ** CURRENT HANGUP:
        run_postproc.py reads postproc_season.ini with configParser, but is not correctly getting any information from it.
            Online search says that a common reason for this is that configParser is passed an incorrect filepath (it fails silently)
            But, I'm fairly certain the filepath being passed is correct, so I'm at a loss.

# K Quigley 2024/08/02
