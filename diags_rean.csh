#!/bin/tcsh
#
# DART software - Copyright UCAR. This open source software is provided
# by UCAR, "as is", without charge, subject to all terms of use at
# http://www.image.ucar.edu/DAReS/DART/DART_download
#
# This script runs interactively or as a batch job. If run as a batch job,
# be careful not to change the data_scripts.csh before this job is running.
# The year and month come from this resource file.
#
# This script runs the obs_diag program on a months' worth of obs_seq.final files 
# from a CESM/DART experiment. The target year/month are the ones defined in the
# CASEROOT/data_scripts.csh resource file. This file also provides the locations
# of the obs_seq.final files as well as the pieces used to construct the output
# directory for the obs_diag_output.nc files, and a possible archive directory.
#
# The observations files have been moved by the short-term archiver to the
# location specified by data_DOUT_S_ROOT/esp/hist and new output directories will
# be created for each year-month in this same location.  To prevent overwriting any 
# possible existing output, if the output directory exists, the script exits before
# doing anything harmful. This script also requires that there is an input.nml (template)
# in the same directory as the observations.
#
# Resources I want:
#    select=#nodes
#    ncpus=#CPUs/node
#    mpiprocs=#MPI_tasks/node
#    -m ae Send email after a(bort) or e(nd)
#    -o    Send standard output and error to this file. It's helpful to use the $casename here.
#
#PBS  -N diags_rean
#PBS  -A NCIS0006
#PBS  -q share
#PBS  -l select=1:ncpus=1:mpiprocs=1
#PBS  -l walltime=04:00:00
#PBS  -m ae
#PBS  -M raeder@ucar.edu
#PBS  -o diags_rean.eo
#PBS  -j oe 
#PBS  -k eod 
#--------------------------------------------

if ($?PBS_O_WORKDIR) then
   cd $PBS_O_WORKDIR
endif

if (! -f data_scripts.csh) then
   echo "ERROR: no data_scripts.csh.  Submit from the CASEROOT directory"
   exit 1
endif

# Get CASE variables from the central resource file.

source ./data_scripts.csh
if ($status != 0) then
   echo "ERROR: unable to read resource file 'data_scripts.csh'."
   exit 2
endif

echo "data_month       = $data_month"
echo "data_year        = $data_year"
echo "data_proj_space  = ${data_proj_space}"
echo "data_DART_src    = ${data_DART_src}"
echo "data_DOUT_S_ROOT = ${data_DOUT_S_ROOT}"
echo "data_CASEROOT    = ${data_CASEROOT}"
echo "data_CASE        = ${data_CASE}"

set     yymm = `printf %4d-%02d $data_year $data_month`
set diag_dir = ${data_DOUT_S_ROOT}/esp/hist/Diags_NTrS_${yymm}
set proj_dir = ${data_proj_space}/${data_CASE}/esp/hist/${yymm}

echo "diag_dir = ${diag_dir}"
echo "proj_dir = ${proj_dir}"

# Check for the existence of the output directory

if (! -d ${diag_dir}) then
   mkdir ${diag_dir}
else
   echo "ERROR: ${diag_dir} exists; choose another name"
   exit 3
endif

cd ${diag_dir}
echo "Running in ${diag_dir}, starting at "`date`

# Create the list of obs_sequence files for this month.
# ls -1 does not work; unusable formatting.

ls ../*obs_seq_final*${yymm}*[^rz] >! obs.list 
if ($status != 0) then
   echo "ERROR: Making obs.list failed."
   echo "ERROR: Unable to create list of input observation files. Exiting"
   exit 4
endif

# Create the namelist with the expected year,month

if (-e ../input.nml) then
   echo "using ../input.nml" 
   cp ../input.nml .
else
   echo "ERROR: can't find ${diag_dir}/../input.nml"
   exit 5
endif

if ($data_month == 12) then
   @ year_last = $data_year + 1
   @ mo_last = 1
else
   @ year_last = $data_year
   @ mo_last = $data_month + 1
endif

ex input.nml<< ex_end
/obs_diag_nml/
/obs_sequence_name/
s;= '.*';= "";
/obs_sequence_list/
s;= '.*';= "./obs.list";
/first_bin_center/
s;=  BOGUS_YEAR, 1;=  $data_year,$data_month;
/last_bin_center/
s;=  BOGUS_YEAR, 2;=  $year_last,$mo_last;
wq
ex_end

# If running interactively, this is a chance to review the input.nml.

if ($?PBS_O_WORKDIR) then
else
   vi input.nml
endif

echo "Running ${data_DART_src}/models/cam-fv/work/obs_diag"
              ${data_DART_src}/models/cam-fv/work/obs_diag >&! obs_diag.out 
set ostat = $status
if ($ostat != 0) then
   echo "ERROR: obs_diag failed.  Exiting"
   exit 6
endif
if ( -e obs_diag_output.nc ) then
   echo "SUCCESS: ${diag_dir}/obs_diag_output.nc created at "`date`
endif

# If possible, archive all the obs_seq_finals for this month in a tar file
# that exists in the project storage directory.

if (! -d ${proj_dir}) then
   mkdir ${proj_dir}
else
   echo "WARNING: ${proj_dir} exists."
   echo "WARNING: Refusing to archive on top of existing data."
   exit 7
endif

set obs_seq_tar = ${data_CASE}.cam_obs_seq_final.${yymm}.tgz 
tar -czvf ${proj_dir}/${obs_seq_tar} ../*obs_seq*${yymm}*
if ($status != 0) then
   echo "ERROR: tar of obs_seq_finals failed.  Exiting"
   exit 8
endif

if ( -e ${proj_dir}/${obs_seq_tar} ) then
   echo "SUCCESS: ${proj_dir}/${obs_seq_tar} created at "`date`
endif

# Keep in mind that (at this point) there are no other copies of the 
# final observation sequence files.
# If you are in a bind for space and feel comfortable that if the tar
# files is created you can safely delete the observation files: 
# rm *obs_seq*${yymm}*

exit 0

