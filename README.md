# DESGW-automation_pipeline

    The goal of this pipeline is to string together the various scripts needed for GW optical follow-up (DAGMaker, SE processing, Post-Processing) so that it runs as a single package with minimal interference needed.  As of 2024/07/31, the pipeline seems to be in mostly working order, but is not entirely complete.

CONTENTS:
    
    The only files and directories needed to run the pipeline are those prefixed with "auto-".  These are:
        ./auto-main.sh
        ./auto-main_pipeline.sh
        ./auto-exposures.list
        ./auto-SE_prep.py
        ./auto-write_DAGMaker.sh
        ./auto-SEproc_scripts
        ./auto-PostProc.py
        ./auto-PostProc_scripts
    Other directories will be created throughout the course of the pipeline.  These are:
        ./SE_proc
        ./SE_output
        ./PostProc
        ./PostProc_output

RUNNING THE PIPELINE:
    
    ./auto-main.sh contains all the terminal commands needed to run the pipeline.
    User must be logged in as the desgw user on des70.fnal.gov
    User must specify:
        HOME_DIR    where the pipeline will be run (this is the automation_pipeline directory path)
        SEASON      season number of the analysis
        EXP_NUMS    space-separated list of exposure numbers to be processed, passed in single quotation marks
    
    ************************
    * When user is logged in and these three variables are defined, all that needs to be done is to run 
    *
    *    auto-main.sh
    *    
    * in the terminal from any location.  No arguments are required.  
    ************************

    All other necessary containers, scripts, and packages will be automatically imported and set up.

    Alternatively, ./auto-main_pipeline.sh may be run directly with SEASON and EXP_NUMS passed as arguments.
    An example of this option is given in the final line of ./auto-main.sh

DOCUMENTATION AND NOTES:
    
    ./auto-main.sh
        This is the only file that needs to be run for the pipeline.  
            It can be run from any location, as it will cd to the HOME_DIR
            User must be logged in as desgw on the des70 machine for the pipeline to work. 
        Defines variables, and runs ./auto-main_pipeline.sh

        Here is where HOME_DIR, SEASON, EXP_NUMS, and other arguments may be defined.
        Contains an example of how to log in as the desgw user on des70.fnal.gov
        Contains an example of how to run ./auto-main_pipeline.sh directly.
        
    ./auto-main_pipeline.sh
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

        SE (Single-Exposure) Processing:
            Creates ./SE_proc directory if it doesn't already exist.
            Runs ./auto-SE_prep.py
                Reads ./auto-exposures.list
                    This is identical to the regular exposures.list
                Collects BAND, SEASON, and PROPID for each exposure in EXP_NUMS
                Records this information in ./SE_output/output.txt, which will be created if it doesn't already exist.
                    This will be read by ./auto-PostProc.py
                Exposures are sorted into bands, and a list of bands is created.
                    This is so that SE processing can be partially parallelized.
            Runs ./auto-write-DAGMaker.sh within ./SE_proc
                Creates ./SE_proc/dagmaker.rc for the season
            
            Each band (u, g, r, i, z, y, etc) is run in parallel:
                Exposures within each band are run in series:
                    A directory is created within ./SE_proc for each exposure, if it doesn't already exist.
                    This exposure directory contains the essential contents of the gw-workflow folder and dagmaker.rc for the season.

                    DAGMaker.sh is run within the exposure directory if it has not been run already.
                        ** This is NOT identical to gw-workflow/DAGMaker.sh
                            Edits made:
                                If template image in dCache doesn't match des51.b, the des51.b version is copied into dCache.
                                    This is to remedy files which have been corrupted or interrupted during writing.
                                If template images are not available in dCache, the only backup check is in des51.b
                                    This is because des30.b is now depreciated and des41.b is not a general fix.
********************************    The NCSA fetch currently returns a 403: FORBIDDEN error(!)  **************************************
                                    The NOAO fetch was never implemented on the original DAGMaker.sh
                                If template images are still incomplete after this check, they are skipped.
                                    Line 694, "${SKIP_INCOMPLETE_SE}" == "false" changed to "${SKIP}" == "true"
                                The copy command is always "idfh cp", instead of switching to "cp" for the desgw user.
                                    "cp" does not seem to correctly copy des51.b shortcuts into dCache.
                        This line is run within a container.
                            The full mount list is actually required for purposes of template image retrieval.
                        Produces log dag_out.out in exposure directory.
                        Produces a dag job file for the exposure.
                    
                    DAG job is submitted from within the exposure directory if it has not been submitted already.
                        DAG job is submitted outside of the container for security reasons.
                        Uses jobsub client, which is only available on des70.fnal.gov
                        Actual file that is run is SEdiff.sh
                            ** This is similar but NOT identical to gw-workflow/SEdiff.sh
                                Edits made:
                                    ./expCalib-isaac-BBH.py and ./expCalib-isaac-BNS.py are copied into the job folder earlier
                        Produces a jobsub out file for the exposure.
                            The bottom of this file gives a ########.0@jobsub0#.fnal.gov location.
                            This location can be used to check on the status of the job at 
                                https://fifemon.fnal.gov/monitor/d/000000115/job-cluster-summary 
            
****    In the future it would be wise to put a timer wait between SE processing and Post-Processing (!)    **************************

        Post-Processing:
            Creates a ./PostProc directory if it doesn't already exist.
            This PostProc directory contains the essential contents of the Post-Processing folder and ./auto-PostProc.py
************    It is not yet entirely certain if all of the essential contents of Post-Processing are actually present (!)    *******
            Runs ./auto-PostProc.py within ./PostProc inside a container.
                ** This is similar but NOT identical to DESGW-pipeline_automation/postprocessing_automations.py
                    Edits made:
                        Various minor fixes.
                        Removal of git pull of Post-Processing folder, since these files are already included within the pipeline.
                        Removal of user prompting for ID and exposure numbers, since these should be passed in already.
                        configParser package removed; the work it did has now been coded by hand.
                            The loaded version is for Python 2.7 and does not run correctly.
                Takes in arguments LIGOID, TRIGGERID, TRIGGERMJD from ./auto-main_pipeline.sh
                    SEASON, EXPOSURES, and PROPID are read from ./SE_output
                Detects whether SE processing was successful for each CCD, or where it failed.
                Creates a completed exposure list and a postproc ini file for the season.
                If there are enough completed CCDs and exposures to move on, it continues with Post-Processing.
                Runs diffimg_setup.sh within ./PostProc
****************    If this is meant to setup and import everything needed for run_postproc.py, is is not successful.    *************
****************    Probably what needs to be done is to make a new file ./auto-PostProc_setup.sh which sets up and imports what is needed for all of Post-Processing, then runs ./auto-PostProc.py    *******************************************************************
                Runs run_postproc.py within ./PostProc, which in turn runs postproc.py
****************    Fails to import pandas correctly, which means that a ./auto-PostProc_setup.sh is probably actually needed.    ****
                Creates a ./PostProc_output directory for final results.

CURRENT STATUS:
    
    Pipeline is completely successful at SE Processing setup and submitting SE Processing job in parallel.
    Pipeline is completely successful at Post-Processing setup.
    The pipeline has mostly been tested on very new and very old GW events
        These exposures tend to have template images which are missing from dCache and des51.b
        This leads to a large number of "fits file not found" or "failed to read fits" errors.
            Ultimately these lead to errors involving "xtalk" files.
    There is at this moment a large number of jobs being held, which is preventing possible SE tests from finishing.
    Because there have been no successful SE Processing jobs yet, the pipeline is untested on the Post-Processing section.

K Quigley 2024/07/31
