import argparse
import os

#get season and exposures
parser = argparse.ArgumentParser()
parser.add_argument('--season') 
parser.add_argument('--expnums') 
args = parser.parse_args()
args.expnums = args.expnums.split(" ")

exposure_file = open('./auto-exposures.list', 'r')

#read through exposure list to get band and propid for each exposure
exposure_data = []
prop_data = set()
band_data = []
for line in exposure_file.readlines():
    line_entries = line.split()
    if line_entries[0] in args.expnums:
        exposure_data.append(str(line_entries[0] + " " + line_entries[5]))
        band_data.append([line_entries[0], line_entries[5]])
        prop_data.add(line_entries[7])

#record exposure, band, propid, and season for PostProcessing
if "SE_output" not in os.listdir("."): 
    os.makedirs("./SE_output")
out_file = open("./SE_output/output.txt", "w")
out_file.write(str(exposure_data) + "\n")
out_file.write(args.season + "\n")
if len(prop_data) == 1:
    out_file.write(prop_data.pop() + "\n")
else:
    print("echo 'Exposures have different prop ids.  Job will fail in PostProcessing.' ;")

#sort exposures by band and declare a list of exposures in each band, and a list of band lists for parallelization
sorted_exposures = dict()
for exposure in band_data:
    if exposure[1] in sorted_exposures.keys():
        sorted_exposures[exposure[1]].append(exposure[0])
    else:
        sorted_exposures[exposure[1]] = [exposure[0]]

for band in sorted_exposures.keys():
    print('export EXPOSURES_' + band + '=(' + ' '.join(sorted_exposures[band]) + ') ;')
print('export BANDS=(EXPOSURES_' + ' EXPOSURES_'.join(sorted_exposures.keys()) + ') ;')