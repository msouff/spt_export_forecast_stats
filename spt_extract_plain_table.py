################################################################
#
# File: spt_extract_plain_table.py
# Author: Michael Souffront
# Date: 11/30/2017
# Purpose: Calculate basic statistics for GloFAS-RAPID files and
#          extract them to a summary table
# Requirements: NCO, netCDF4
#
################################################################

import os
import subprocess as sp
import netCDF4 as nc
import datetime as dt

def extract_summary_table(workspace):
    # calls NCO's nces function to calculate ensemble statistics for the max, mean, and min
    for stat in ['max', 'avg', 'min']:
        findstr = 'find "{0}" -name Qout*.nc'.format(workspace)
        filename = os.path.join(workspace, 'nces.{0}.nc'.format(stat))
        ncesstr = "nces -O --op_typ={0} {1}".format(stat, filename)
        args = ' | '.join([findstr, ncesstr])

        sp.call(args, shell=True)

    # creates list with the stat netcdf files created in the previous step
    nclist = []
    for file in os.listdir(workspace):
        if file.startswith("nces"):
            nclist.append(os.path.join(workspace, file))

    # creates file name for the csv file
    date_string = os.path.split(workspace)[1].replace('.', '')
    full_name = os.path.split(os.path.split(workspace)[0])[1]
    file_name = 'summary_table_{0}_{1}.csv'.format(full_name, date_string)

    # creates dictionary with return periods
    d = {}
    return_periods_file = os.path.join(os.path.split(workspace)[0], 'return_periods.csv')
    with open(return_periods_file, 'rb') as f:
	lines = f.readlines()
	lines.pop(0)
	for line in lines:
	    d[line.split(',')[0]] = line.split(',')[1:4]

    # creates a csv file to store statistics
    with open(os.path.join(workspace, file_name), 'wb') as f:
        # writes header
        #f.write('id,watershed,subbasin,comid,return2,return10,return20,index,timestamp,max,mean,min,style\n')

        # extracts forecast COMIDS and formatted dates into lists
        comids = nc.Dataset(nclist[0], 'r').variables['rivid'][:].tolist()
        rawdates = nc.Dataset(nclist[0], 'r').variables['time'][:].tolist()
        dates = []
        for date in rawdates:
            dates.append(dt.datetime.utcfromtimestamp(date).strftime("%m/%d/%y %H:%M"))

        # creates empty lists with forecast stats
        maxlist = []
        meanlist = []
        minlist = []

        # loops through the stat netcdf files to populate lists created above
        for ncfile in sorted(nclist):
            res = nc.Dataset(ncfile, 'r')

            # loops through COMIDs with netcdf files
            for index,comid in enumerate(comids):
                if 'max' in ncfile:
                    maxlist.append(res.variables['Qout'][index, 0:49].tolist())
                elif 'avg' in ncfile:
                    meanlist.append(res.variables['Qout'][index, 0:49].tolist())
                elif 'min' in ncfile:
                    minlist.append(res.variables['Qout'][index, 0:49].tolist())

        # creates step order list
        step_order = range(1,50)

	# creates watershed and subbasin names
        watershed_name = full_name.split('-')[0]
        subbasin_name = full_name.split('-')[1]

	# creates unique id
	count = 1

        # loops through COMIDs again to add rows to csv file
        for index,comid in enumerate(comids):
            for step, date, max, mean, min in zip(step_order, dates, maxlist[index], meanlist[index], minlist[index]):
		# define style
		if mean > float(d[str(comid)][2]):
		    style = 'purple'
		elif mean > float(d[str(comid)][1]):
		    style = 'red'
		elif mean > float(d[str(comid)][0]):
		    style = 'yellow'
		else:
		    style = 'blue'
                f.write(','.join([str(count), watershed_name, subbasin_name, str(comid), d[str(comid)][0], 
				  d[str(comid)][1], d[str(comid)][2], str(step), date, str(max), str(mean), 
				  str(min), style + '\n']))
		count += 1

    return('Success')


# runs function on file execution
if __name__ == "__main__":
    # output directory
    workdir = '/home/byuhi/rapid-io/output/'
    # list of watersheds
    watersheds = [os.path.join(workdir, d) for d in os.listdir(workdir) if os.path.isdir(os.path.join(workdir, d))]
    for watershed in watersheds:
        # list of available dates
        dates = [os.path.join(watershed, d) for d in os.listdir(watershed) if os.path.isdir(os.path.join(watershed, d))]
        for date in dates:
            extract_summary_table(
                workspace=date
            )

