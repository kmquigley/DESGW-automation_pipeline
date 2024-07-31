#!/usr/bin/env python

# In[ ]:
import argparse
import os
import glob
import datetime
from collections import Counter

# In[ ]:
#accept arguments
parser = argparse.ArgumentParser(description=__doc__, 
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument('--ligoid')
parser.add_argument('--triggerid')
parser.add_argument('--triggermjd')

args = parser.parse_args()

# In[ ]:
# Get a list of exposures, season, and propid
# Check if they've finished in image processing pipeline
# Document and report what hasn't
# Figure out why it didn't finish it
# Build appropriate postproc.ini file
# Check if user wants to run from scratch or start wherever the pipeline failed last
# If the latter, deduce where the pipeline finished off last, set as SKIPTO flag
# Run post-processing pipeline

# In[ ]:

#Read file outputted from image processing to get season, exposure, band, and propid
img_proc_file = open("../SE_output/output.txt")
lines = img_proc_file.readlines()

exposures = lines[0].strip()
exposures = exposures[1:-1]
exposures = list(exposures.split(","))
for exp in range(len(exposures)):
    exposures[exp] = exposures[exp].replace("'","")

season = lines[1].strip()

propid = lines[2].strip()

print('Season: ' + season)
print('Exposures: ' + str(exposures))
print('Propid: ' + str(propid))

# In[ ]:

# Get exposure, format: /pnfs/des/persistent/gw/exp/NITE/EXPOSURE_NUMBER/dpSEASON/BAND_CCD
# Check CCDs from image processing, check forcephoto files
# Output list of exposures that can move on to post processing

dir_prefix = '/pnfs/des/persistent/gw/exp/'
dpSeason = ('dp' + str(season) + '/')    
    
exposures_to_cont = []
bandslist = []

for exposure in exposures:
    exposure = exposure.split()
    band = exposure[1]
    exposure = exposure[0]
    
    if band not in bandslist:
        bandslist.append(band)
    
    #term_size = os.get_terminal_size()
    print('=' * 12)
    print("\nFOR EXPOSURE " + str(exposure) + ":\n")
    exposure_dir = dir_prefix + '*' + '/' + exposure +'/' + dpSeason + band
    band_dirs = glob.glob(exposure_dir + '_*' + '/') #what we're counting to make sure they're all there
    print('There are '+ str(len(band_dirs)) + ' ' + str(band) + ' ccds\n')

    if glob.glob(dir_prefix + '*' + '/' + exposure +'/' + dpSeason + 'input_files'):
        print('input_files found\n')
    else:
        print('input files not found\n')
    
    complete_ccds=0
    incomplete_ccds=0
    failed_ccds=0
    complete_ccds_list = []
    fail_files = []

    for dir in band_dirs:
        if glob.glob(dir+'*.FAIL'):
            fail_file = str(glob.glob(dir+'*.FAIL'))
            fail_file = str(fail_file.split('/')[-1:])
            fail_file = fail_file.strip('[]"')
            print('ccd ' + dir[-5:-1] + ' failed on ' + fail_file)
            failed_ccds += 1
            fail_files.append(fail_file)

        elif glob.glob(dir + 'outputs_*'):
            complete_ccds += 1
            complete_ccds_list.append(dir[-5:-1])
        else:
            print('ccd ' + dir[-5:-1] + ' incomplete')
            incomplete_ccds += 1

    print('\nThere are ' + str(failed_ccds) + ' failed ' + str(band) + ' ccds')        
    print('There are ' + str(incomplete_ccds) + ' incomplete ' + str(band) + ' ccds')
    print('There are ' + str(complete_ccds) + ' complete '+ str(band) + ' ccds')

    print('\n'+str(Counter(fail_files)))
    
    if complete_ccds >= 50:
        print('\nover 50 ' + str(band) + ' ccds completed: acceptable')
        num_ccds = complete_ccds
    else:
        print('\nnot enough ccds in exposure ' + str(exposure) + ' for post processing\n')
        continue

# Get forcephoto exposure, format: /pnfs/des/persistent/gw/forcephoto/images/dpSEASON/NITE/EXPOSURE/

    expected_forcephoto_files = num_ccds * 2
    print('\nexpected forcephoto files for band ' + str(band) + ': ' + str(expected_forcephoto_files))

    forcephoto_dir_prefix = '/pnfs/des/persistent/gw/forcephoto/images/'

    forcephoto_dir = forcephoto_dir_prefix + dpSeason + '*' + '/' + exposure + '/'
    forcephoto_files = glob.glob(forcephoto_dir + '/' + '*' + '_' + str(band) + '_' + '*')
    print('found forcephoto files for exposure '+  str(exposure) + ': ' + str(len(forcephoto_files)) + '\n')
    if len(forcephoto_files) == expected_forcephoto_files: 
        print('all forcephoto files completed in exposure ' + str(exposure) + ' -> transferring to post processing\n')
        exposures_to_cont.append(exposure)
    elif len(forcephoto_files) < expected_forcephoto_files and len(forcephoto_files) > expected_forcephoto_files / 2 :
        print('some forcephoto files not yet completed in exposure ' + str(exposure) + '\n')
        exposures_to_cont.append(exposure)
        for num in complete_ccds_list:
            if not glob.glob(forcephoto_dir + '*' + str(num) + '*.fits') or not (forcephoto_dir + '*' + str(num) + '*.psf'):
                print('forcephoto files for ' + num + ' not completed / missing')
            #elif glob.glob(forcephoto_dir + '*' + num + '*.fits') and glob.glob(forcephoto_dir + '*' + num + '*.psf'):
                #print('exposure ' + num + ' completed')
        print('\nover 50% forcephoto files in exposure ' + exposure + ' completed -> transferring to post processing\n')
    elif len(forcephoto_files) < expected_forcephoto_files and not len(forcephoto_files) > expected_forcephoto_files / 2 :
        print('fewer than 50% forcephoto files completed, will not add to exposures.list')
        continue
    elif len(forcephoto_files) > expected_forcephoto_files:
        print('check: More forcephoto files than expected for this exposure -> will not add to exposures.list')
        continue
    
print('exposures moving to post processing:\n' + str(exposures_to_cont))

# In[ ]:

#create custom exposure.list file
print('creating .list file for completed exposures\n')

current_exposures = 'complete_exposures' + '_S' + str(season) + '_' + str(datetime.datetime.now().strftime("%Y%m%d_%H-%M")) + '.list'
with open(current_exposures, 'w') as f:
    for exposure in exposures_to_cont:
        f.write("%s\n" % exposure)

# In[ ]:

#prepare variables for .ini file
if args.ligoid == "None": ligoid = None
else: ligoid = args.ligoid

if args.triggerid == "None": triggerid = None
else: triggerid = args.triggerid

if args.triggermjd == "None": triggermjd = None
else: triggermjd = args.triggermjd

exposures_listfile = str(current_exposures)

bandslist = str(bandslist)
bandslist = bandslist.strip("[]'")
bandslist = bandslist.replace("'","")
bandslist += ' ;'

#create .ini file
print('creating .ini file with completed exposures list\n')

os.system('cp ./postproc.ini ./postproc_' + str(season) + '.ini')
postproc_season_file = './postproc_'+ str(season) + '.ini'

template_ini_file = open('./postproc.ini', 'r')
season_ini_file = open(postproc_season_file, 'w')

#this has to be done manually because our configParser is built for Python 2.7
writeline = True
for line in template_ini_file.readlines():
    if "season = " in line:     
        writeline = False                                        
        season_ini_file.write("season = " + str(season) + "\n")

    elif "ligoid = " in line:
        writeline = False
        if ligoid != None:         
            season_ini_file.write("ligoid = " + str(ligoid) + "\n")

    elif "triggerid = " in line:
        writeline = False
        if triggerid != None:         
            season_ini_file.write("triggerid = " + str(triggerid) + "\n")

    elif "propid = " in line: 
        writeline = False
        season_ini_file.write("propid = " + str(propid) + "\n")

    elif "triggermjd = " in line:
        writeline = False
        if triggermjd != None:         
            season_ini_file.write("triggermjd = " + str(triggermjd) + "\n")

    elif "exposures_listfile = " in line: 
        writeline = False
        season_ini_file.write("exposures_listfile = " + str(exposures_listfile) + "\n")

    elif  "bands = " in line: 
        writeline = False
        season_ini_file.write("bands = " + str(bandslist) + "\n")


    if writeline == True: season_ini_file.write(line)

    if "outdir = " in line: outdir = line.split(" = ") [-1]
    if "plusname = " in line: truthplusfile = line.split(" = ") [-1]

# In[ ]:

#Check if we want to SKIPTO

if glob.glob('./' + outdir[2:] + '/makedatafiles/LightCurvesReal/*.dat'):
    SKIPTO_flag = 6
    print('\nWill run post processing from step 6')
elif os.path.exists('./' + outdir[2:] + '/truthtable'+str(season)+'/'+truthplusfile): #output from step 4

    SKIPTO_flag = 5
    print('\nWill run post processing from step 5')

else:
    print('No evidence of steps already completed in post processing, will not skip')

print('\nContinuing to post processing')


# In[ ]:

#setup for Post Processing
print('running diffimg_setup.sh\n')
os.system('. ./diffimg_setup.sh')

print('running forcephoto\n')
os.system('. ./update_forcephoto_links.sh')
    
print('running postproc\n')
#run_postproc.py
try:
    SKIPTO_flag
except NameError:
    print("\nRunning run_postproc.py\n")
    os.system('nohup python run_postproc.py --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out ')
else:
    print("\nRunning run_postproc.py with skip\n")
    os.system('nohup python run_postproc.py --SKIPTO ' + str(SKIPTO_flag) + ' --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out ')