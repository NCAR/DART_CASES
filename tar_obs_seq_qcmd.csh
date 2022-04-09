#!/bin/tcsh


# >>> Adapt for living in $caseroot instead of $pr/archive/esp/hist


#  Usage: edit this file to change date
#         > remote tar_obs_seq_qcmd.csh

set year_mm  = 2020-01
set case     = f.e21.FHIST_BGC.f09_025.CAM6assim.011
# set proj_dir = "/glade/p/nsc/ncis0006/Reanalyses/${case}/esp/hist/${year_mm}"
set proj_dir = "/glade/scratch/raeder/${case}/project/esp/hist/${year_mm}"
set file     = "${case}.cam_obs_seq_final.${year_mm}.tgz"

if (! -d $proj_dir) mkdir -p $proj_dir

set cmnd = "tar -c -z -f ${proj_dir}/${file} "'*obs_seq*'"${year_mm}"'*'

cd /glade/scratch/raeder/${case}/archive/esp/hist

echo "$cmnd" >& tar_obs_seq.${year_mm}.log.$$
qcmd -A P86850054 -l walltime=6:00:00 -q share \
     -l select=1:ncpus=1:mpiprocs=1 \
     -- "$cmnd"  >>& tar_obs_seq.${year_mm}.log.$$
# 
