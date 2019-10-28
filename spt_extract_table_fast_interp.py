#################################################################
#
# File: spt_extract_plain_table.py
# Author(s): Michael Souffront, Wade Roberts, Spencer McDonald
# Date: 03/07/2018
# Purpose: Calculate basic statistics for GloFAS-RAPID files and
#          extract them to a summary table; linearly interpolates 
#          forecast values for time steps other than 3 hrs
# 
# Requirements: NCO, netCDF4, numba
#################################################################

import os
import subprocess as sp
import netCDF4 as nc
import datetime as dt
import numpy as np
from numba import jit, prange


def extract_summary_table(workspace):
    # calls NCO's nces function to calculate ensemble statistics for the max, mean, and min
    for stat in ['max', 'avg', 'min']:
        findstr = 'find "{0}" -name Qout*.nc'.format(workspace)
        filename = os.path.join(workspace, 'nces.{0}.nc'.format(stat))
        ncesstr = "/home/wade/miniconda3/envs/py2k/bin/nces -O --op_typ={0} {1}".format(stat, filename)
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

    # creating pandas dataframe with return periods
    d = {}
    return_periods_path = os.path.join(os.path.split(workspace)[0], '{0}-return_periods.csv'.format(full_name))
    with open(return_periods_path, 'r') as f:
        lines = f.readlines()
        lines.pop(0)
        for line in lines:
            d[line.split(',')[0]] = line.split(',')[1:4]

    # creates a csv file to store statistics
    with open(os.path.join(workspace, file_name), 'wb') as f:
        # writes header
        f.write('id,watershed,subbasin,comid,return2,return10,return20,index,timestamp,max,mean,min,style,flow_class\n')

        # extracts forecast COMIDS and formatted dates into lists
        comids = nc.Dataset(nclist[0], 'r').variables['rivid'][:].tolist()
        first_date_ts = nc.Dataset(nclist[0], 'r').variables['time'][0].tolist()
        first_date = dt.datetime.utcfromtimestamp(first_date_ts)
        dates = [(first_date + dt.timedelta(hours=x)).strftime("%m/%d/%y %H:%M") for x in range(0, 361, 3)]

        # creates empty lists with forecast stats
        maxlist = []
        meanlist = []
        minlist = []

        # loops through the stat netcdf files to populate lists created above
        for ncfile in sorted(nclist):
            res = nc.Dataset(ncfile, 'r')

            # loops through COMIDs with netcdf files
            for index, comid in enumerate(comids):
                if 'max' in ncfile:
                    maxlist.append(res.variables['Qout'][index].tolist())
                elif 'avg' in ncfile:
                    meanlist.append(res.variables['Qout'][index].tolist())
                elif 'min' in ncfile:
                    minlist.append(res.variables['Qout'][index].tolist())

        max_array = np.reshape(np.array(maxlist), (-1, 85))
        mean_array = np.reshape(np.array(meanlist), (-1, 85))
        min_array = np.reshape(np.array(minlist), (-1, 85))

        max_array_interp = interpolate(max_array, 48)
        mean_array_interp = interpolate(mean_array, 48)
        min_array_interp = interpolate(min_array, 48)

        maxlist = max_array_interp.tolist()
        meanlist = mean_array_interp.tolist()
        minlist = min_array_interp.tolist()

        # creates step order list
        step_order = range(1, 122)

        # creates watershed and subbasin names
        watershed_name = full_name.split('-')[0]
        subbasin_name = full_name.split('-')[1]

        # creates unique id
        count = 1

        # loops through COMIDs again to add rows to csv file
        for index, comid in enumerate(comids):
            for step, date, max_val, mean_val, min_val in zip(step_order, dates, maxlist[index], meanlist[index],
                                                              minlist[index]):
                # define style
                if mean_val > float(d[str(comid)][2]):
                    style = 'purple'
                elif mean_val > float(d[str(comid)][1]):
                    style = 'red'
                elif mean_val > float(d[str(comid)][0]):
                    style = 'yellow'
                else:
                    style = 'blue'

                # define flow_class
                if mean_val < 20:
                    flow_class = '1'
                elif 20 <= mean_val < 250:
                    flow_class = '2'
                elif 250 <= mean_val < 1500:
                    flow_class = '3'
                elif 1500 <= mean_val < 10000:
                    flow_class = '4'
                elif 10000 <= mean_val < 30000:
                    flow_class = '5'
                else:
                    flow_class = '6'

                f.write(','.join([str(count), watershed_name, subbasin_name, str(comid), d[str(comid)][0],
                                  d[str(comid)][1], d[str(comid)][2], str(step), date, str(max_val), str(mean_val),
                                  str(min_val), style, flow_class + '\n']))
                count += 1

    return 'Stat Success'


@jit(nopython=True)
def interpolate(array, c_start):  # c_start is column to start interpolating in (zero indexed)
    num_rows = array.shape[0]
    num_cols = array.shape[1]
    num_interp_columns = num_cols - 1 - c_start

    final_array = np.empty((num_rows, num_cols + num_interp_columns))

    # Populate the Portion That is not interpolated
    for i in prange(num_rows):
        for j in range(c_start + 1):
            final_array[i, j] = array[i, j]

    z = 1
    for j in prange(c_start + 2, num_cols + num_interp_columns, 2):
        for i in range(num_rows):
            final_array[i, j] = array[i, j - z]
        z += 1

    # Interpolate
    for j in prange(c_start + 1, num_cols + num_interp_columns - 1, 2):
        for i in range(num_rows):
            final_array[i, j] = (final_array[i, j - 1] + final_array[i, j + 1]) / 2

    return final_array


# runs function on file execution
if __name__ == "__main__":

    # output directory
    workdir = r'/home/wade/Interpolation_For_Michael/Work_Directory'

    # list of summary table paths
    interpolation_list = []

    # list of watersheds
    watersheds = [os.path.join(workdir, d) for d in os.listdir(workdir) if os.path.isdir(os.path.join(workdir, d))]

    # run summary table
    for watershed in watersheds:
        # list of available dates per watershed
        dates = [os.path.join(watershed, d) for d in os.listdir(watershed) if os.path.isdir(os.path.join(watershed, d))]
        for date in dates:
            extract_summary_table(
                workspace=date
            )
