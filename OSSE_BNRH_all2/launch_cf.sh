#!/bin/bash
#
# DART software - Copyright UCAR. This open source software is provided
# by UCAR, "as is", without charge, subject to all terms of use at
# http://www.image.ucar.edu/DAReS/DART/DART_download
#
# $Id$

# for command file jobs.
# Sidd Ghosh Feb 22, 2017
# Slurm added by Kevin Raeder July 6, 2019
# Updated to check existence of env. vars. wanted, instead of job env. vars.; KR Oct 28, 2022

# On casper's PBS the PMI_RANK variable is not defined,
# but OMPI_COMM_WORLD_RANK is. 
# The system launch_cf.sh tests directly for -z {env_var_name}
# The path of launch_cf.sh (/glade/u/apps/ch/opt/usr/bin) is loaded by default 
#    on cheyenne only (2022-10), so this script must be used on casper, 
#    or load the path into the script that calls it.

if [ ! -z "$PMI_RANK" ]; then
   line=$(expr $PMI_RANK + 1)
#    echo "launch_cf.sh using PMI_RANK with line = $line"
elif [ ! -z "$OMPI_COMM_WORLD_RANK" ]; then
   line=$(expr $OMPI_COMM_WORLD_RANK + 1)
#    echo "launch_cf.sh using OMPI_COMM_WORLD_RANK with line = $line"
else
   echo "Batch environment is unknown"
   exit 11
fi

INSTANCE=$(sed -n ${line}p $1)

# The following command showed that 563 tasks are launched within .3 seconds.
# echo "launching $INSTANCE at "; date --rfc-3339=ns

eval "$INSTANCE"

# <next few lines under version control, do not edit>
# $URL$
# $Id$
# $Revision$
# $Date$
