#!/bin/tcsh

# DART software - Copyright UCAR. This open source software is provided
# by UCAR, "as is", without charge, subject to all terms of use at
# http://www.image.ucar.edu/DAReS/DART/DART_download

#==========================================================================

# Script to package files found in $DOUT_S_ROOT
# after case.st_archive has sorted them,
# and obs_diag has generated a basic obs space diagnostics NetCDF file.
# The resulting files will be moved to 
#   X a project space for further analysis and use
#   > Campaign Storage for intermediate archiving, 
# Both destinations take time.  They are actually copies.

# FIXME KEVIN ... usage notes, inherits from data_scripts.csh, etc. ...
#     >>> edit data_scripts.csh to set the correct rpointer.atm_0001 date.
#         Keep the rpointers organized by naming them rpointer.atm_0001.YYYY-MM-DD-SSSSS
#     >>> Import obs space tgz from Mac
# >>> USAGE NOTES:                                                 <<<
# >>> Run st_archive and obs_diag before running this script.      <<<
# >>> Check that there's enough disk space.                        <<<
#     It needs 1 Tb more than current usage.                       <<<
#     That's assuming that processing the cpl hist files is first, <<<
#     after which lots of space is freed up.  If it's not,         <<<
#     the lnd history files needs almost 3 Tb additional space.    <<<
# >>> submit this script from the CESM CASEROOT directory.         <<<

#
#==========================================================================
#
#PBS  -N repack_st_arch.csh
#PBS  -A P86850054
# #PBS  -q casper
# develop jobs can use up to 2 nodes(!)
#PBS  -q develop
# #PBS  -l job_priority=economy
# casper requests are limited (2022-4) to 144 cpus.
# Derecho has 128 processors/node.
# For forcing: 80        = 80 /128 = 1 
# For hist: 80           = 80 /128 = 1
# For state space: 2 * 4 =  8 /128 = 1  #stages * #stats 
# For rest: 80 + 1       = 81 /128 = 1
# For obs space: 1       =  1  (tar of obs_seq_finals is now done before this script)
# 235 Gb limit on develop queue nodes.
#PBS  -l select=1:ncpus=81:mpiprocs=81:ompthreads=1:mem=150GB
# Tarring a day of restarts to $campaign takes < 3 min.
# There's a 6 hour limit on develop jobs.
#PBS  -l walltime=02:00:00
# Can only make output consistent with SLURM (2011-2019 files)
# by copying the eo file to a new name at the end.
#PBS  -o repack_st_arch_2018-01.eo
#PBS  -j oe 
#PBS  -k eod
#PBS  -m ae
#PBS  -M raeder@ucar.edu
echo "==================================================================="

# Get CASE environment variables from the central variables file.
source ./data_scripts.csh
set ds_status = $status
if ($ds_status != 0) then
   echo "data_scripts.csh failed with error $ds_status"
   exit 2
endif
echo "data_CASE     = $data_CASE"
echo "data_year     = $data_year"
echo "data_month    = $data_month"
echo "data_NINST    = $data_NINST"
echo "data_CASEROOT    = $data_CASEROOT"
echo "data_DOUT_S_ROOT = $data_DOUT_S_ROOT"
echo "data_proj_space  = $data_proj_space"
echo "data_campaign    = ${data_campaign}"

if ($?PBS_O_WORKDIR) then
   cd $PBS_O_WORKDIR
   echo "JOBID = $PBS_JOBID"
# Don't need after the first time, when debugging.    env | sort | grep PBS
endif

# In order to rebase the time variable NCO needs these modules
module load nco udunits

setenv date_rfc 'date --rfc-3339=ns'

# Different commands on cheyenne versus casper (mpirun).
if (`which mpiexec > /dev/null; echo $status` == 0) then
   set mpi_cmd = "mpiexec"
else if (`which mpirun > /dev/null; echo $status` == 0) then
   set mpi_cmd = "mpirun"
endif

echo "Preamble at "`$date_rfc`

if (! -f CaseStatus) then
   echo "ERROR: this script must be run from the CESM CASEROOT directory"
   exit 1
endif

set line           = `grep '^[ ]*stages_to_write' input.nml`
set stages_all     = (`echo $line[3-$#line] | sed -e "s#[',]# #g"`)
echo "stages_all = ($stages_all)"

set line           = `grep '^[ ]*inf_flavor' input.nml | sed -e "s#[',]# #g"`
set inflation      = (`echo $line[3] `)
set inf_log = "log"
# In bash {} must have > 1 items, and a ',}' matches all files.
if ($inflation != 0) set inf_log =  "{dart.r,"${inf_log}"}"
echo "inf_log list = '$inf_log'"

# Non-esp history output which might need to be processed.
# "components" = generic pieces of CESM (used in the archive directory names).
# "models" = component instance names (models, used in file names).
# set components     = (lnd  atm ice  rof)
# set models         = (clm2 cam cice mosart)
set components     = (atm )
set models         = (cam )

set line = `grep -m 1 save_rest_freq ./assimilate.csh`
set save_rest_freq = $line[4]

if (! -d $data_DOUT_S_ROOT) then
   echo "ERROR: Missing local archive directory (DOUT_S_ROOT).  "
   echo "       Maybe you need to run st_archive before this script"
   exit 10
endif

# Default mode; archive 5 kinds of data.
# These can be turned off by editing or argument(s).
# The project space directories are only created if at least one of
#     forcing, obs_space, or history is turned on.
#     Zagar; everything is put directly in campaign storage.
# Number of tasks required by each section (set according to the max of the 'true's)
# do_forcing     => nens + 1
# do_restarts    => nens + 1
# do_obs_space   => 1
# do_history     => nens * MAX(# history file types.)
# do_state_space => 1  (Could be upgraded to use #rest_dates(4-5) * #stats(4))

# This script creates the cmds_template before processing the forcing files,
# so that it doesn't need them.
set do_forcing     = 'false'
# > > > WARNING; if restarts fails when $mm-01 is a Monday, turn off the pre_clean flag,
#                in order to preserve what's in rest/YYYY-MM.
set do_restarts    = 'true'
set do_obs_space   = 'false'
set do_history     = 'true'
set do_state_space = 'true'

# Check whether there is enough project disk space to run this.
# The numbers added to pr_used were harvested from running repack_hwm.csh.
set line = `gladequota | grep -m 1 scratch || exit 1 `
set pr_used = `echo $line[2] | cut -d'.' -f1`
# Round it up to be safe.
@ pr_used++
if (do_forcing == 'true') then
   @ pr_need = $pr_used + 2
   if ($pr_need > 29) then
      echo "ERROR; not enough project space to run this"
      exit 2
   endif
else if ($do_history == 'true') then
   @ pr_need = $pr_used + 1
   if ($pr_need > 29) then
      echo "ERROR; not enough project space to run this"
      exit 3
   endif
endif

#--------------------------------------------
if ($#argv != 0) then
   # Request for help; any argument will do.
   echo "Usage:  "
   echo "Before running this script"
   echo "    Run st_archive and obs_diags.csh. "
   echo "    Check that there's enough free scratch space (3 Tb should do it). "
   echo "    submit this script from the CESM CASEROOT directory. "
   echo "Can't Call by user or script: needs multiple processors"
   exit
endif

# User submitted, independent batch job (not run by another batch job).
# "data_proj_space" will be created as needed (assuming user has permission to write there).
#    So do I need proj_space?  Make it campaign;  eliminate need for repack_project.csh.
#    Make it archive?  Could be fine; work is done in $inst subdirs of hist
# > > > WARNING: if the first day of the month is a Monday,
#       I need to add *_0001.log* files from $archive/logs to rest/YYYY-MM-01-00000
#       and remove the rpointer and .h0. files.  Also compress the .r. files.
set yr_mo = `printf %4d-%02d ${data_year} ${data_month}`

cd $data_DOUT_S_ROOT
pwd

# Check that this script has not already run completely for this date.
if ($do_state_space == true && \
    -f ${data_campaign}/${data_CASE}/logs/${yr_mo}/da.log.${yr_mo}.tar) then
   echo "ERROR; ${data_campaign}/${data_CASE}/logs/${yr_mo}/da.log.${yr_mo}.tar already exists."
   exit 15
endif

set obs_space  = Diags_NTrS_${yr_mo}
if ($do_obs_space != 'true') then
   echo "SKIPPING archiving of obs_space diagnostics"
else if (! -d esp/hist/$obs_space && ! -f esp/hist/${obs_space}.tgz) then
   echo "ERROR: esp/hist/$obs_space does not exist."
   echo "       run obs_diags.csh before this script."
   exit 20
endif

# Make a template cmd file to append this month's time series to the yearly file in $project
# Start with a of all the instances of one file type.
# This doesn't need to be in cpl/hist, but changing that requires changes in sections further down.
echo "------------------------"
if (! -d ${data_DOUT_S_ROOT}/cpl/hist) mkdir -p ${data_DOUT_S_ROOT}/cpl/hist
cd ${data_DOUT_S_ROOT}/cpl/hist

if (-f cmds_template) mv cmds_template cmds_template_prev
touch cmds_template

set i = 1
while ($i <= $data_NINST)
   set INST = `printf %04d $i`
      # When we have project space outside of my /scratch:
      #    set inst_dir = ${data_proj_space}/${data_CASE}/cpl/hist/${INST}
      # Otherwise, data_proj_space has $data_CASE/{archive or project} in its name.
   set inst_dir = ${data_proj_space}/cpl/hist/${INST}
   # "TYPE" will be replaced by `sed` commands below.
   set yr_mo_file = ${data_CASE}.cpl_${INST}.TYPE.${yr_mo}.nc

   if (! -d $inst_dir) then
      # Don't need Previous since no yearly files.  But leave it if it is not in the way.
      mkdir -p ${inst_dir}/Previous
      set init = ''
   else
      cd ${inst_dir}

      ls  ${data_CASE}.cpl_${INST}.*.${yr_mo}.nc >& /dev/null
      if ($status == 0) then
         mv   ${data_CASE}.cpl_${INST}.*.${yr_mo}.nc Previous || exit 28
         # "$init" is a place holder, in the template command, for the existence
         # of a yearly file
         set init = ${inst_dir}/Previous/$yr_mo_file
      else
         set init = ''
      endif

      cd ${data_DOUT_S_ROOT}/cpl/hist
  
   endif

   # First try:
   # echo "ncrcat -A -o $yr_mo_file " \
   #    "${data_CASE}.cpl_${NINST}.TYPE.${yr_mo}-*.nc &> TYPE_${NINST}.eo " \
   # Apparently casper's ncrcat is not described well by NCO 4.8.1 documentation.
   # 1) It treats -A as (over)writing variables into a file,
   #    rather than appending records to a file.  The latter is done by --rec_apn.
   #    My mistake; the docs do say -A is different from concatenating.
   # 2) If the -o option is used, it does not preserve the time:units attribute 
   #    of the first input file, even though it should.  So the output file 
   #    must be listed after the all of the input files.
   # 3) The output file cannot also be the first input file,
   #    or the time monotonicity may be violated.
   #    This defeats the intent of the "append" mode, but testing confirmed it.

   echo "ncrcat $init  ${data_DOUT_S_ROOT}/cpl/hist/${data_CASE}.cpl_${INST}.TYPE.${yr_mo}*.nc " \
        " ${inst_dir}/$yr_mo_file &> TYPE_${INST}.eo " \
        >> cmds_template
   @ i++
end
cd ${data_DOUT_S_ROOT}

echo "------------------------"
if ($do_forcing == true) then
   echo "Forcing starts at "`date`
   cd ${data_DOUT_S_ROOT}/cpl/hist

   # Make a list of the dates (buried in file names).
   set files_dates = `ls ${data_CASE}.cpl_0001.ha2x1d.${yr_mo}-*.nc*`
   if ($#files_dates == 0) then
      echo "ERROR: there are no ${data_CASE}.cpl_0001.ha2x1d files.  Set do_forcing = false?"
      exit 23
   endif
   # Separate decompress.csh for each date which needs it.
   foreach d ($files_dates)
      if ($d:e == 'gz') then
         set ymds = $d:r:r:e
         ${data_CASEROOT}/compress.csh gunzip $ymds "hist" "not_used"
         if ($status != 0) then
            echo "ERROR: Compression of coupler history files failed at `date`"
            exit 25
         endif
      endif
   end

   # Append a copy of the template file, modified for each file type, into the command file.
   if (-f mycmdfile) mv mycmdfile mycmdfile_prev
   touch mycmdfile
   @ task = 0
   foreach type (ha2x3h ha2x1h ha2x1hi ha2x1d hr2x)
      sed -e "s#TYPE#$type#g" cmds_template >> mycmdfile
      @ task = $task + $data_NINST
   end

   echo "   forcing mpirun launch_cf.sh starts at "`date`
   $mpi_cmd -n $task ${data_CASEROOT}/launch_cf.sh ./mycmdfile
   set mpi_status = $status
   echo "   forcing mpirun launch_cf.sh ends at "`date`

   ls *.eo >& /dev/null
   if ($status == 0) then
      grep ncrcat *.eo >& /dev/null
      # grep failure = ncrcat success = "not 0"
      set ncrcat_failed = $status
   else
      # No eo files = failure of something besides ncrcat.
      echo "cmdfile created no log files for forcing files "
      echo "   and mpi_status of ncrcats = $mpi_status"
      set ncrcat_failed = 0
   endif

   if ($mpi_status == 0 && $ncrcat_failed != 0) then
      rm mycmdfile *.eo $inst_dir:h/*/Previous/*.${type}.*
   else
      echo "ERROR in repackaging $type forcing (cpl history) files: See h\*.eo, cmds_template, mycmdfile"
      echo '      grep ncrcat *.eo  yielded status '$ncrcat_failed
      exit 50
   endif

   cd ${data_DOUT_S_ROOT}
endif

# 2) Restart sets, weekly on Monday @ 00Z
#    $DOUT_S_ROOT/rest/YYYY-MM-DD-00000
#    Package by member and date, to allow users 
#      to grab only as many members as needed.
#      a la ./package_restart_members.csh
#    Send to campaign storage using ./mv_to_campaign.csh
# This requires space in $scratch to house the new tar files.
# The files must exist there until globus is done copying them to campaign storage.
# If $scratch has filled with assimilation output, then the forcing file
# section of this script will make enough room for this section.

echo "------------------------"
if ($do_restarts == true) then
   echo "Restarts starts at "`$date_rfc`
   
   cd ${data_DOUT_S_ROOT}/rest

   # Pre_clean deals with the feature of mv_to_campaign.csh,
   # which copies all the contents of a directory to campaign storage.
   # If the contents of that directory are left over from a previous repackaging,
   # the directory needs to be cleaned out.
   # It needs to be true for the first run, to make the inst directories.
   # During debugging, it may be helpful to *not* clean out the source directory.
   set pre_clean = false


   # Files_to_save keeps track of whether any restart sets have been packaged
   # for moving to campaign storage.
   set files_to_save = false


   foreach rd (`ls -d ${yr_mo}-*`)
      # Purge restart directories which don't fit in the frequency 
      # defined in setup_* by save_every_Mth_day_restart.
#       echo ' '
#       echo Contents of rest at the start of the dates loop
#       ls -dlt *

      # Prevent archiving files (just do directories).
      if (-f $rd) continue
 
      # The directories we want have only numbers and '-'s.
      echo $rd | grep '[a-zA-Z]' 
      if ($status == 0) continue

      # Ignore directories names that don't have '-'.
      # This doesn't apply to the $yr_mo (mv_to_campaign.csh) directory
      # because it didn't exist when the set of $rd was defined for this loop.
      echo $rd | grep '\-'
      if ($status != 0) continue

      echo ' '
      echo Processing $rd

      set rd_date_parts = `echo $rd | sed -e "s#-# #g"`
      set day_o_month = $rd_date_parts[3]
      set sec_o_day   = $rd_date_parts[4]
      echo year = $data_year

      if ($pre_clean == true) then
         set pre_clean = false

         # Mv_to_campaign.csh is designed to move everything in a directory to campaign storage.
         # Clean up and/or make a new directory for the repackaged files 
         # and send that directory to mv_to_campaign.csh.
   
         if (-d $yr_mo) then
            rm ${yr_mo}/*
            echo "Cleaned out contents of previous restarts from $yr_mo"
         else
            mkdir $yr_mo
            echo "Made directory `pwd`/$yr_mo "
            echo "   to store restart members until globus archives them."
         endif
 
      endif

      set purge = 'true'

      # Learn whether save_rest_freq is a string or a number.
      # Character strings must be tested outside of the 'if' statement.
      echo $save_rest_freq | grep '[a-z]'
      if ($status == 0) then
         set purge_date = ${yr_mo}-${day_o_month}
         set weekday = `date --date="$purge_date" +%A`
         if ($weekday == $save_rest_freq && \
             $sec_o_day == '00000') set purge = 'false'
   
      # Numbers can be tested inside the 'if' statement.
      else if (`echo $save_rest_freq | grep '[0-9]'`) then
         if ($day_o_month % $save_rest_freq == 0 && \
             $sec_o_day == '00000') set purge = 'false'

      else
         echo "ERROR: save_every_Mth_day_restart = $save_rest_freq from setup_??? is not supported.  "
         echo "   It must be an integer (< 31) or a day of the week (Monday).  Exiting"
         exit 57 
      endif
   
      if ($purge == 'true') then
         echo "Ignoring restart directory $rd because it doesn't match "
         echo "         save_every_Mth_day_restart = $save_rest_freq and/or $sec_o_day != 00000"
         echo "         It will be removed by purge.csh."

      # This prevents the most recent (uncompressed) restart set from being archived
      # But this may be prevented more reliably by the selection in foreach ( $yr_mo).
      else 
         # YYYY-MM-01-00000 directories are archived by st_archive instead of by assimilate.csh,
         # so they don't have CESM log files in them, which will cause the code below to crash.
         ls ${rd}/*.log.*  >& /dev/null
         if ($status != 0 ) then
            echo "ERROR: $data_DOUT_S_ROOT/rest/${rd} is not optimized for archiving."
            echo "       1) Remove the rpointer and .h0. files."
            echo "       2) Import log files from archive/logs, "
            echo "          ls -l *0001.i.*   to get the time stamp of the .i. "
            echo "          look for it in logs, "
            echo "          cp ../../logs/{*_0001,cesm}.log.*{YYMMDD-hhmm}* . "
            echo "       3) Compress the *.r.* files (probably with compress.csh)."
            echo "       4) Then rerun this."
            echo "       5) If it fails again (e.g. rest dir could not be removed),"
            echo "          set pre_clean = false."
            exit 59
         endif
      
         echo "Exporting restart file set ${rd} "
         echo "   to ${data_campaign}/${data_CASE}/rest/${yr_mo} "
         mkdir -p ${data_campaign}/${data_CASE}/rest/${yr_mo}

         set files_to_save = true

         if (-f mycmdfile) then
            mv mycmdfile mycmdfile_prev
            rm *.eo
         endif
         touch mycmdfile
         set i = 1
# When the first day of the month is a Monday (or whatever's set in setup_advance),
# st_archive archives those files to rest without compressing them.
# There could be a compression here as part of tarring them:
#          set comp_arg = ''
#          if ($day_o_month == 01) comp_arg = ' -z'
#          ...
#          echo " tar ${comp_arg} -c ...
# This would still result in tar files with a different size 
# for the first day of the month, but would save space (~67 Gb /month).
# Note that these commands are run in sh, not csh, so the output redirection 
# has a different(?) form; &>.
         while ($i <= $data_NINST)
            set INST = `printf %04d $i`
            # echo "tar -c -f ${yr_mo}/${data_CASE}.${INST}.alltypes.${rd}.tar "                 \
         
            echo "tar -c -f ${data_campaign}/${data_CASE}/rest/${yr_mo}/${data_CASE}.${INST}.alltypes.${rd}.tar " \
                           "${rd}/${data_CASE}.*_${INST}.*.${rd}.* &>  tar_${INST}_${rd}.eo "  \
                     "&& rm ${rd}/${data_CASE}.*_${INST}.*.${rd}.* &>> tar_${INST}_${rd}.eo" >> mycmdfile
            @ i++
         end
         # Clean up the rest (non-instance files).
         # 'dart.r' will catch the actual inflation restart files (.rh.)
         # and the restart file (.r.), which is used only by st_archive.
         # Assimilate.csh should always put the .rh. files into the restart sets,
         # so there's no 'if' test around this command (although it might be useful
         # to print a meaningful error message).
         # It's necessary to direct the error output from the rm to the eo file
         # so that it can be examined below and won't necessarily cause an error+exit below.
         # OSSE; this tar works even if there are no dart.r files.
         
         echo "tar -c -f ${data_campaign}/${data_CASE}/rest/${yr_mo}/${data_CASE}.infl_log.alltypes.${rd}.tar " \
                        "${rd}/*$inf_log* &>  tar_inf_log_${rd}.eo "  \
                  "&& rm ${rd}/*$inf_log* &>> tar_inf_log_${rd}.eo " >> mycmdfile
         @ tasks = $data_NINST + 1
      
         echo "Restart mpirun launch_cf.sh starts at "`$date_rfc`
         $mpi_cmd -n $tasks ${data_CASEROOT}/launch_cf.sh ./mycmdfile
         set mpi_status = $status
         echo "launch_cf.sh mpi_status = " $mpi_status

         set num_tars = `ls -1 ${data_campaign}/${data_CASE}/rest/${yr_mo}/*${rd}* | wc -l`
         # num_tars doesn't need [1] because standard output was piped to wc, 
         # instead of wc operating on a file.
         if ($num_tars != $tasks) set mpi_status = $tasks
         echo "Restart mpirun launch_cf.sh ends at "`$date_rfc`" with status "$mpi_status
    
         set tar_failed = 0
         ls *.eo >& /dev/null
         if ($status == 0) then
            # Look for tar error messages
            grep tar tar_*.eo | grep -v log >& /dev/null
            # grep failure = tar success = "not 0"
            set tar_failed = $status
         endif
      
         if ($mpi_status == 0 && $tar_failed != 0) then
#             echo "Would have removed *.eo, ${rd}/files, if it were working"
            rm tar*.eo
         else
            echo 'ERROR in repackaging restart files: See tar*.eo, mycmdfile'
            echo '      grep tar tar*.eo  yielded status '$tar_failed
            ls -l *.eo
            exit 60
         endif

      endif

# NOW I'm tarring directly to campaign; don't copy separately.
#       # Copy all the restart tars to campaign storage
#       # It's OK to do this within the loop because cp -u copies only the new(er) files
#       # to campaign storage.
#       if ($files_to_save == true) then
#          # Remove the empty directory to prevent mv_to_campaign.csh from archiving it.
#          rmdir $rd
#          if ($status != 0) then
#             echo "ERROR; $rd is not empty, Cannot remove it"
#             exit 62
#          endif
#          rm mycmdfile
#    
#          # Echo the archive command to help with globus error recovery
#          # and make it easier to do that on cheyenne as well as on casper.
#          #   Replace with rsync?   Probably don't need that complication.
#          if (! -d ${data_campaign}/${data_CASE}/rest) mkdir -p ${data_campaign}/${data_CASE}/rest
#          echo " cp -u -r ${data_DOUT_S_ROOT}/rest/$yr_mo "
#          echo "          ${data_campaign}/${data_CASE}/rest"
#          cp -u -r ${data_DOUT_S_ROOT}/rest/$yr_mo \
#                   ${data_campaign}/${data_CASE}/rest >&! cpstatus &
#          set cpid = $!
#          wait
#          grep cp cpstatus >& /dev/null 
#          set cpstatus = $status
#          ps -p $cpid >& /dev/null
#          # If cp is no longer running and 'cp' did not appear in the cp output (aka no error);
#          if ($status != 0 && $cpstatus != 0) then
#          # Will be done by purge.csh    rm -rf ${data_DOUT_S_ROOT}/rest/${yr_mo}/*
#             rm ${rd}/${data_CASE}.*.${rd}.* 
#             rm ${rd}/*{dart.r,log}* 
#          else
#             echo "cp of rest failed"
#             exit 70
#          endif
#       endif
   end

   cd ${data_DOUT_S_ROOT}
   
endif


# 3) Obs space diagnostics.
#    Generated separately using ./obs_diags.csh

echo "------------------------"
if ($do_obs_space == true) then
   cd ${data_DOUT_S_ROOT}/esp/hist
   echo " "
   echo "Location for obs space is `pwd`"
   
   # This is single threaded and takes a long time,
   #  so do_obs_space is usually 'false'
   # and this tar is done in the script that also calls obs_diag: 
   # cesm2_1/diags_rean.csh, >>> But OSSEs used ~/Scripts/diags_batch.csh,
   # which doesn't tar the obs_seq files.
   # Or, for archiving that's out of the usual flow (2020-01+COSMIC2),
   # manually tar the files in $archive/esp/hist and copy the tar file to Campaign Storage.
   tar -z -c -f ${data_CASE}.cam_obs_seq_final.${yr_mo}.tgz \
         ${data_CASE}.dart.e.cam_obs_seq_final.${yr_mo}* &

   # Move the obs space diagnostics to $project.
   set obs_proj_dir = ${data_proj_space}/esp/hist/${yr_mo}
   if (! -d $obs_proj_dir) mkdir -p $obs_proj_dir

   mv ${obs_space}.tgz $obs_proj_dir
   if ($status == 0) then
# Z_OSSE      rm -rf $obs_space
   else if (! -f ${obs_proj_dir}/${obs_space}.tgz) then
      echo "$obs_space.tgz could not be moved.  $obs_space not removed"
   endif

   set obs_seq_root = ${data_CASE}.cam_obs_seq_final.${yr_mo}
   echo "Waiting for tar of obs_seq files at date - `date --rfc-3339=ns`"
   wait
   if (  -f ${obs_seq_root}.tgz && \
       ! -z ${obs_seq_root}.tgz) then
      mv ${obs_seq_root}.tgz     $obs_proj_dir
      if ($status == 0) then
# Z_OSSE remove manually later         rm ${obs_seq_root}*
         echo "Moved ${obs_seq_root}.tgz to $obs_proj_dir"
      endif
   else if (! -f ${obs_proj_dir}/${obs_seq_root}.tgz) then
      echo "${obs_seq_root}.tgz cannot be moved"
      exit 90
   endif

   if (! -d ${data_campaign}/${data_CASE}/esp/hist) mkdir -p ${data_campaign}/${data_CASE}/esp/hist
   echo " cp -u -r ${obs_proj_dir} ${data_campaign}/${data_CASE}/esp/hist"
   cp -u -r ${obs_proj_dir} ${data_campaign}/${data_CASE}/esp/hist >&! cpstatus &
   set cpid = $!
   wait
   grep cp cpstatus >& /dev/null 
   set cpstatus = $status
   ps -p $cpid >& /dev/null
   if ($status != 0 && $cpstatus != 0) then
#  This is done by purge.csh       rm $obs_proj_dir/*
   else
      echo "cp of esp/hist failed"
      exit 70
   endif

   cd ${data_DOUT_S_ROOT}
   
endif

#--------------------------------------------

# 4) CESM history files
#    All members should be saved for Zagar; use cmdfile for h files.
#    concatenate all of the h1 files (per month; 120 * .67 Mb = 80 Mb).
#    This should archive the stages (.e.) files too.   (30/mo; only -00000)
#       Just copy them. 85 Mb each.  a month would be 2.5 Gb.
#       They compress to 71 Mb; 20%

echo "------------------------"
if ($do_history == true) then
   # If cam files are too big, do them separately and monthly.
   echo "There are $#components components (models)"
   # Near the beginning of the script:
   # set components     = (lnd  atm ice  rof)
   # set models         = (clm2 cam cice mosart)
   set m = 1
   while ($m <= $#components)
      ls $components[$m]/hist/*.h0.* >& /dev/null
      if ($status != 0) then
         echo "\n Skipping $components[$m]/hist"
         @ m++
         continue
      endif
# Zagar_OSSE; keep all cam files
#       if ($models[$m] == 'cam' ) then
#          # Look only for files which are after possible leftovers from the previous month.
#          ls $components[$m]/hist/*.h[^0].${yr_mo}-02* >& /dev/null
#          if ($status != 0) then
#             echo "\n Skipping $components[$m]/hist"
#             @ m++
#             continue
#          endif
#       endif

      cd $components[$m]/hist
      echo " "
      echo "Source location for history is `pwd`"

      if (-f cpcmdfile) rm cpcmdfile
      touch cpcmdfile
      set i = 1
      # This odd definition may have been useful when some members completed
      # but the job was interrupted, so I wanted to continue with i != 1.
      @ comp_ens_size = ( $data_NINST - $i ) + 1
      while ($i <= $data_NINST)
         set INST = `printf %04d $i`
         set inst_dir = ${data_proj_space}/$components[$m]/hist/${INST}
# debug
         ls -l $inst_dir

# This returns true, even when the ls fails.
# but not in a small test program.
#         if (-d $inst_dir) then
         if ($status == 0) then
            echo "ERROR: OSSE; $inst_dir already exists and may have files I want to keep"
            exit 94

         else 
            mkdir -p $inst_dir
            # Copy all of the stages for this instance and this month to
            # inst_dir; campaign storage; the destination, as a command file
            # Apparently the * don't need to be escaped, as seen in cmds_template.
            # (h* are ncrcatted directly to $campaign.)
            # >>> I may want thes original lines when $data_proj_space is not = $data_campaign
            # echo "cp -u -r *${INST}.e.*.${yr_mo}* ${INST}/*.${yr_mo}*  $inst_dir " \
            #      " && rm   *${INST}.e.*.${yr_mo}* ${INST}/*.${yr_mo}* " \
            echo "cp -u -r *${INST}.e.*.${yr_mo}*   $inst_dir  &>  cp_allhist_${INST}.eo " \
                 " && rm   *${INST}.e.*.${yr_mo}*  &>> cp_allhist_${INST}.eo" \
                 >> cpcmdfile

         endif

         @ i++
      end
   
      # Make a cmd file to append this month's time series to the yearly file in $project
      # Start with a template of all the instances of one file type.
   
      # This is what's in cmds_template, which is re-used here.
      #       set inst_dir = ${project}/${data_CASE}/cpl/hist/${INST}
      #       set yr_mo_file = ${data_CASE}.cpl_${INST}.TYPE.${yr_mo}.nc
      #       echo "ncrcat --rec_apn    $yr_mo_file " \
      #            "${data_CASE}.cpl_${INST}.TYPE.${yr_mo}-*.nc ${inst_dir}/$yr_mo_file &> " \
      #            "TYPE_${INST}.eo " \

      echo "Checking the template used to make a local cmds file."
      # Safer test; this confirms that cmds_template is not leftover from the previous month.
      grep -m 1 $yr_mo ${data_DOUT_S_ROOT}/cpl/hist/cmds_template
      if ($status != 0) then
         echo "ERROR: ${data_DOUT_S_ROOT}/cpl/hist/cmds_template is missing; need it for archiving h# files."
         echo "       It should have been created in section 1 of this script."
         exit 105
      endif
   
      set templ_size = `wc -l ${data_DOUT_S_ROOT}/cpl/hist/cmds_template`
      if ($templ_size[1] != $comp_ens_size) then
         echo "ERROR: Mismatch of sizes of ${data_DOUT_S_ROOT}/cpl/hist/cmds_template "
         echo "       and this component's members = $comp_ens_size"
         exit 110
      endif

      set cmds_template = cmds_template_$models[$m]
      sed -e "s#cpl_#$models[$m]_#g;s#cpl#$components[$m]#g"  \
          ${data_DOUT_S_ROOT}/cpl/hist/cmds_template >! $cmds_template

      # Put a copy of the template file, modified for each file type, into the command file.
      set mycmdfile = mycmdfile_$models[$m]

      # The number of history files = SUM(data_NINST * hist_types_this_comp * dates_this_type)
      @ type = 0
      while ($type < 10)
         # This learns when there are no more h# types to process.
         # All the desired dates for this type will be appended 
         # to the yearly file for EACH member.
         # Mosart writes out monthly h0 files by default.
         # I don't know whether they have actual monthly averages,
         # or just the last time slot.  There are .rh0 files in $rundir.
         # In any case, they're labeled with YYYY-MM, but no -DD-SSSSS.
         # In contrast to the forcing file list of dates, this list does
         # not include the last "-" because we want to find the Mosart files,
         # but there's no danger of finding the yearly file because
         # even if $project = $data_DOUT_S_ROOT/archive, the yearly files are 
         # accumulated in instance subdirectories named $INST.
         set dates = `ls *0001.h${type}.${yr_mo}*`
         if ($#dates == 0) break
   
         # There are data_NINST commands in cmds_template.
         # If cam.h0 ends up with more than PHIS, don't do this if test.
         # and fix the h0 purging in the state_space section.
         if ($models[$m] == 'cam' && $type == 0) then
          # If a single/few cam.h0 files need to be saved (for PHIS):
            sed -e "s#TYPE#h$type#g" ${cmds_template} | grep _0001 >> ${mycmdfile}
            @ tasks = 1
            echo "history: cam h0 will have only 1 member (contains only PHIS)"
         else
            sed -e "s#TYPE#h$type#g" ${cmds_template} > ${mycmdfile}
            @ tasks = $data_NINST
         endif
   
         if (-z $mycmdfile) then
            # Local (hist files) cmds_template, not the original one from before do_forcing.
            rm ${cmds_template}  ${mycmdfile} 
            echo "Skipping $components[$m]/hist type $type because $mycmdfile has size 0"
            break
         endif

         echo "   history mpirun launch_cf.sh starts at "`$date_rfc`
         $mpi_cmd -n $tasks ${data_CASEROOT}/launch_cf.sh ./${mycmdfile}
         set mpi_status = $status
         echo "   history ncrcats mpirun launch_cf.sh ends at "`$date_rfc`
 
         echo "Checking for the existence of cmdfile error files, which would mean 'stop'"
         ls *.eo >& /dev/null
         if ($status == 0) then
            grep ncrcat *.eo >& /dev/null
            # grep failure = ncrcat success = "not 0"
            set ncrcat_failed = $status
         else
            echo "cmdfile created no log files for history files "
            echo "   and mpi_status of ncrcats = $mpi_status"
            set ncrcat_failed = 0
         endif
      
         if ($mpi_status == 0 && $ncrcat_failed != 0) then

            if (! -d    ${data_campaign}/${data_CASE}/$components[$m]/hist) \
               mkdir -p ${data_campaign}/${data_CASE}/$components[$m]/hist
#             >>> Replaced with cmdfile after types=0...10 loop 
#             cp -u -r 00* ${data_campaign}/${data_CASE}/$components[$m]/hist  >&! cpstatus &
#             set cpid = $!
#             wait
#             grep cp cpstatus >& /dev/null 
#             set cpstatus = $status
#             ps -p $cpid >& /dev/null
#             if ($status != 0 && $cpstatus != 0) then
#                rm -rf ${cmds_template}  ${mycmdfile} *.eo $inst_dir:h/*/Previous/* \
# # Don't remove 00*; that's only done at the end of a year by ?
# # 00*  *$models[$m]_*.h*.${yr_mo}*
#             endif
         else
            echo "ERROR in repackaging history files: See $components[$m]/hist/"\
                 'h*.eo, cmds_template*, mycmdfile*'
            echo '      grep ncrcat *.eo  yielded status '$ncrcat_failed
            exit 130
         endif

         @ type++
      end

      # Copy the component file types (stage's ensembles and h* yr_mo files) 
      # to Campaign Storage.  cpcmdfile is defined in the first section of do_hist.
      echo "   history copy mpirun launch_cf.sh starts at "`$date_rfc`
      $mpi_cmd -n $data_NINST ${data_CASEROOT}/launch_cf.sh ./cpcmdfile
      set mpi_status = $status
      echo "   history copy mpirun launch_cf.sh ends at "`$date_rfc`

      echo "Checking for the existence of cmdfile error files, which would mean 'stop'"
      ls *.eo >& /dev/null
      if ($status == 0) then
         grep cp *.eo >& /dev/null
         # grep failure = cp success = "not 0"
         set cp_failed = $status
      else
         echo "cmdfile created no log files for history files "
         echo "   and mpi_status of copies = $mpi_status"
         set cp_failed = 0
      endif

      if ($mpi_status == 0 && $cp_failed != 0) then
         echo "Successful copies of history files to $data_proj_space $yr_mo"
# debug          rm ${cmds_template} cpcmdfile
      else
         echo "ERROR in repackaging history files: See $components[$m]/hist/"\
              'h*.eo, cmds_template*, mycmdfile*'
         echo '      grep ncrcat *.eo  yielded status '$ncrcat_failed
         exit 130
      endif

      cd ${data_DOUT_S_ROOT}

      @ m++
   end
endif

#--------------------------------------------
# 5) DART diagnostic files: state space
#    + esp/hist/.i.cam_output_{mean,sd}
#    + esp/hist/.e.cam_$stages_{mean,sd}
#    + esp/hist/.rh.cam_$stages_{mean,sd}
#      compress?  85 Mb * 120 dates * 6 uncompressed files= 60 Gb -> 52 Gb/mo.
#                 save 100 Gb / year
echo "------------------------"
if ($do_state_space == true) then
   cd ${data_DOUT_S_ROOT}/esp/hist
   echo " "
   echo "Location for state space is `pwd`"
   
   mkdir $yr_mo
   if (-f mycmdfile) then
      mv mycmdfile mycmdfile_prev
      rm *.eo
   endif
   touch mycmdfile
   set tasks = 0
   foreach stage ($stages_all)
      set ext = e
      if ($stage == output) set ext = i
   
      # Ignoring posterior inflation for now.
# Z_OSSE_TUV_noinf has no inflation files
#       foreach stat  (mean sd priorinf_mean priorinf_sd)
      foreach stat  (mean sd )
         echo $stat | grep inf 
         if ($status == 0) set ext = rh
         echo "stage, stat, ext = $stage $stat $ext"
   
         set files = "${data_CASE}.dart.${ext}.cam_${stage}_${stat}.${yr_mo}"
         if (! -f     ${yr_mo}/$files.tar) then
            echo "tar -c -f ${yr_mo}/${files}.tar ${files}-* " \
                 " &>  tar_${stage}_${stat}.eo" \
                 >> mycmdfile
# I can't make it work with rm.  It keeps trying to find files after they've been removed.
#                  " && rm ${files}-* &>> tar_${stage}_${stat}.eo" \
#                 " && sleep 5; rm ${files}-* &>> tar_${stage}_${stat}_$PMI_RANK.eo" \
            @ tasks++ 
         endif
      end
   end

   echo "State space mpirun launch_cf.sh starts at "`$date_rfc`" on $tasks tasks"
   $mpi_cmd -n $tasks ${data_CASEROOT}/launch_cf.sh ./mycmdfile
   set mpi_status = $status
   echo "State space mpirun launch_cf.sh ends at "`$date_rfc`" with status "$mpi_status

   ls *.eo >& /dev/null
   if ($status == 0) then
      grep tar tar*.eo | grep -v log >& /dev/null
      # grep failure = tar success = "not 0"
      set eo_failure = $status
   else
      # No eo files = failure of something besides tar.
      set eo_failure = 0
      echo "State space file set, nontar eo_failure = $eo_failure"
   endif
   if ($mpi_status == 0 && $eo_failure != 0) then
      rm tar*.eo
   else
      echo 'ERROR in repackaging DART state space files: See tar*.eo, mycmdfile'
      echo '      grep tar tar*.eo  yielded status '$eo_failure
      ls -l *.eo
      exit 70
   endif

   # Echo the archive command to help with globus error recovery
   # and make it easier to do that on cheyenne as well as on casper.
   if (! -d    ${data_campaign}/${data_CASE}/esp/hist) \
      mkdir -p ${data_campaign}/${data_CASE}/esp/hist
   echo " cp -u -r ${data_DOUT_S_ROOT}/esp/hist/${yr_mo} "
   echo "          ${data_campaign}/${data_CASE}/esp/hist"
   cp -u -r ${data_DOUT_S_ROOT}/esp/hist/${yr_mo} \
            ${data_campaign}/${data_CASE}/esp/hist >&! cpstatus &
   set cpid = $!
   wait
   grep cp cpstatus >& /dev/null 
   set cpstatus = $status
   ps -p $cpid >& /dev/null
   if ($status != 0 && $cpstatus != 0) then
      echo "Copy of state space statistics appears to have worked"
      # This is done by purge.csh       
      # rm -rf ${data_DOUT_S_ROOT}/esp/hist/${yr_mo}/*
   else
      echo "cp of esp/hist failed"
      exit 150
   endif
 
# This section was replaced by a new cp command in the history file section,
# since that's where the files live, and it will handle stage's ensembles
# for each component.
# The removed code tarred each instance-stage ensemble into a single file,
# which we don't want for the OSSEs.

#-----------------------------------------------------------------------------
# >>> REMOVE after I've transferred the questions to another repack_st_arch.csh.
#-----------------------------------------------------------------------------
# # The ensemble means are archived every 6 hours, because they're useful and small.
# # It also may be useful to have some complete ensembles of model states,
# # so those are saved less often (weekly, plus some others sneak in).
# # This section archives the ensemble.  
# # The members also have a different "file type" than the means
# # and are archived to atm/hist, instead of esp/hist
#    cd ${data_DOUT_S_ROOT}/atm/hist
#    echo " "
#    echo "Next Location for state space is `pwd`"
#    
# I don't remember why stages_all[1] is used here; the first stage in the list is somewhat arbitrary.
#    set files = `ls ${data_CASE}.cam_0001.e.$stages_all[1].${yr_mo}*`
#    echo "Files from which atm $stages_all[1] allinst dates will be gathered:"
#    echo "  $files" | sed -e "s# #\n#g"
#    if ($#files == 0) then
#       echo "There are no .e.$stages_all[1].${yr_mo} files in atm/hist.  Continuing."
#    else
# 
# Do I really need to assemble a list individually?  Couldn't  wildcards find them?
#       if (! -d $yr_mo) mkdir $yr_mo
#       set dates = ()
#       foreach f ($files)
#          # These files may or may not be compressed, so extracting the date
#          # part of the file name is a bit tricky.
#          set d = $f:r:e
#          if ($d == nc) set d = $f:r:r:e
#          set dates = ($dates $d)
#       end
#       echo " "
#       echo "Archiving atm/hist/{$dates} to Campaign Storage"
#       
#       if (-f mycmdfile) then
#          mv mycmdfile mycmdfile_prev
#          rm *.eo
#       endif
#       touch mycmdfile
# 
#       @ stages = $#stages_all - 1
#       @ tasks = $#dates * $stages
#       foreach d ($dates)
#       foreach stage ($stages_all)
#       if ($stage != output) then
#          echo "tar -c -f ${yr_mo}/${data_CASE}.cam_allinst.e.${stage}.${d}.tar" \
#                                 " ${data_CASE}.cam_[0-9]*.e.${stage}.${d}* &>  tar_${stage}_${d}.eo" \
#                           " && rm ${data_CASE}.cam_[0-9]*.e.${stage}.${d}* &>> tar_${stage}_${d}.eo" \
#               >> mycmdfile
#       endif
#       end
#       end
#    
#       echo "State space, non-output ensembles mpirun launch_cf.sh starts at "`$date_rfc`
#       $mpi_cmd -n $tasks ${data_CASEROOT}/launch_cf.sh ./mycmdfile
#       set mpi_status = $status
#       echo "State space, non-output ensembles mpirun launch_cf.sh ends at "`$date_rfc`\
#            " with status "$mpi_status
#  
#       ls *.eo >& /dev/null
#       if ($status == 0) then
#          grep tar tar*.eo | grep -v log >& /dev/null
#          # grep failure = tar success = "not 0"
#          set ncrcat_failed = $status
#       else
#          # No eo files = failure of something besides tar.
#          set ncrcat_failed = 0
#       endif
#    
#       if ($mpi_status == 0 && $ncrcat_failed != 0) then
#          rm tar*.eo
#       else
#          echo 'ERROR in repackaging nonoutput CAM ensemble files: See tar*.eo, mycmdfile'
#          echo '      grep tar tar*.eo  yielded status '$ncrcat_failed
#          ls -l *.eo
#          exit 80
#       endif
#       
#       # Echo the archive command to help with globus error recovery
#       # and make it easier to do that on cheyenne as well as on casper.
# >>> Change archiving of gci command  to command file
#       echo "gci cput -r ${data_DOUT_S_ROOT}/atm/hist/${yr_mo}: "
#       if (! -d ${data_campaign}/${data_CASE}/atm/hist) mkdir -p ${data_campaign}/${data_CASE}/atm/hist
#       echo "cp -u -r ${data_DOUT_S_ROOT}/atm/hist/$yr_mo "
#       echo "         ${data_campaign}/${data_CASE}/atm/hist"
#       cp -u -r ${data_DOUT_S_ROOT}/atm/hist/$yr_mo \
#                ${data_campaign}/${data_CASE}/atm/hist >&! cpstatus &
#       set cpid = $!
#       wait
#       grep cp cpstatus >& /dev/null 
#       set cpstatus = $status
#       ps -p $cpid >& /dev/null
#       if ($status != 0 && $cpstatus != 0) then
#          # It failed to find the running cp and failed to find an error message.
# # This is done by purge.csh          rm -rf ${data_DOUT_S_ROOT}/atm/hist/${yr_mo}/*
#       else
#          echo "cp of atm/hist failed ($cpstatus =? 0) or was still running" 
#          exit 160
#       endif
#    
#    endif
# 
# END DELETION

   # Archive DART log files (and others?)

   cd ${data_DOUT_S_ROOT}/logs
   
   # Create a list of files to archive.
   # Logs from more components could be added here.
   set list = ()
   foreach f (`ls da.log*`)
      if ($f:e == 'gz') then
         gunzip $f 
         set f = $f:r
      endif
      grep -l "valid time of model is $data_year $data_month" $f >& /dev/null
      if ($status == 0) then
         set list = ($list $f)
      else
         echo "   $f not added to list because 'valid time' $data_year $data_month  not found in it. "
      endif
   end

   if ($#list == 0) then
      echo "WARNING: No log files found to archive."
      echo "         da.log list has no files in it."
   endif

   echo "Archiving "
   echo $list | sed -e "s# #\n  #g"

   if (! -d $yr_mo) mkdir $yr_mo
   tar -z -c -f ${yr_mo}/da.log.${yr_mo}.tgz $list
   if ($status == 0) then
      # Echo the archive command to help with globus error recovery
      # and make it easier to do that on cheyenne as well as on casper.
      if (! -d ${data_campaign}/${data_CASE}/logs) mkdir -p ${data_campaign}/${data_CASE}/logs
      echo "cp -u -r ${data_DOUT_S_ROOT}/logs/${yr_mo} "
      echo "         ${data_campaign}/${data_CASE}/logs"
      cp -u -r ${data_DOUT_S_ROOT}/logs/${yr_mo} \
               ${data_campaign}/${data_CASE}/logs
      if ($status == 0) then
         rm $list
# This is done by purge.csh          rm -rf ${data_DOUT_S_ROOT}/logs/${yr_mo}/*
         echo "cp succeeded, but no local files were removed"
      else
         echo "cp of logs failed"
         exit 170
      endif
   else
      echo "Tar of da.logs of $yr_mo failed.  Not archiving them"
   endif

   cd ${data_DOUT_S_ROOT}

endif 

#--------------------------------------------

cd $data_CASEROOT
if ($?PBS_JOBID) then
   echo "PBS_JOBNAME = $PBS_JOBNAME"
   if (-f $PBS_JOBNAME:r.eo) then
      mv $PBS_JOBNAME:r.eo $PBS_JOBNAME:r_${yr_mo}.eo
   else
      echo "$PBS_JOBNAME:r.eo does not exist, despite PBS -k eod"
   endif
endif

exit 0
