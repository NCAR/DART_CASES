#!/bin/csh
#
# DART software - Copyright UCAR. This open source software is provided
# by UCAR, "as is", without charge, subject to all terms of use at
# http://www.image.ucar.edu/DAReS/DART/DART_download

# ------------------------------------------------------------------------------
# Purpose: assimilate with a CAM ensemble and perform advanced archiving
#          and compression in support of multiple assimilation cycles in a
#          single CESM job.
#
# The (resulting) assimilate.csh script is called by CESM with two arguments:
# 1) the CASEROOT, and
# 2) the assimilation cycle number in this CESM job
# ------------------------------------------------------------------------------
# This template is lightly modified by the setup scripts to be appropriate
# for specific hardware and other configurations. The modified result is
# then given execute permission and is appropriate to use for an assimilation.
# All of this is automatically performed by the DART-supplied setup scripts.
#
# Tag DART's state output with names using CESM's convention:
#    ${case}.${scomp}[_$inst].${filetype}[.$dart_file].${date}.nc
#    These should all be named with $scomp = "cam" to distinguish
#    them from the same output from other components in multi-component assims.
#
# This script also has logic in it to manage disk space in a way that allows
# for more assimilation cycles to be performed before archiving without losing
# critical restart capability. The same logic is also useful for assimilations
# that may require multiple timesteps to be available.
#
# As a specific example, consider the case when 3 assimilation cycles have been
# performed: 6Z, 12Z, 18Z.
# If we want to keep a restart set and a backup
# restart set, we only need the 18Z and 12Z, so the 6Z set can be removed.
# Let's also say that its the last cycle of job - which automatically kicks off
# the short-term archiver. If we did 'nothing', the 12Z and 18Z get archived
# and the 18Z gets restaged

# Echoing env variables set in env_mach_specific.xml to prevent launch failures.
echo ' '
echo "  MPI_LAUNCH_TIMEOUT = $MPI_LAUNCH_TIMEOUT"
echo "  MPI_IB_CONGEST= $MPI_IB_CONGEST"
echo "  MPI_COMM_MAX= $MPI_COMM_MAX"
echo "  TMPDIR= $TMPDIR"
# echo "  MPI_VERBOSE= $MPI_VERBOSE"
# echo "  MPI_VERBOSE2= $MPI_VERBOSE2"
echo ' '

# machine-specific dereferencing

if ($?SLURM_JOB_ID) then

   # SLURM environment variables:
   # env | grep SLURM | sort

   setenv ORIGINALDIR $SLURM_SUBMIT_DIR
   setenv     JOBNAME $SLURM_JOB_NAME
   setenv       JOBID $SLURM_JOBID
   setenv     MYQUEUE $SLURM_JOB_PARTITION
   setenv   NODENAMES $SLURM_NODELIST
   setenv LAUNCHCMD "mpirun -np $SLURM_NTASKS -bind-to core"
#  untested method for determining NUMTASKS_PERNODE with SLURM
#  set ANY_OLD_NODE = `head -n 1 $SLURM_NODELIST`
#  setenv NUMTASKS_PERNODE `grep $ANY_OLD_NODE $SLURM_NODELIST | wc -l`

else if ($?PBS_NODEFILE) then

   # PBS environment variables:
   # env | grep PBS | sort

   setenv ORIGINALDIR $PBS_O_WORKDIR
   setenv     JOBNAME $PBS_JOBNAME
   setenv       JOBID $PBS_JOBID
   setenv     MYQUEUE $PBS_O_QUEUE
   setenv     NUMCPUS $NCPUS
   setenv    NUMTASKS `cat  $PBS_NODEFILE | wc -l`
   setenv    NUMNODES `uniq $PBS_NODEFILE | wc -l`
   set ANY_OLD_NODE = `head -n 1 $PBS_NODEFILE`
   setenv    NUMTASKS_PERNODE `grep $ANY_OLD_NODE $PBS_NODEFILE | wc -l`
   setenv  MPIEXEC_MPT_DEBUG 0
   setenv MP_DEBUG_NOTIMEOUT yes
   setenv          LAUNCHCMD mpiexec_mpt

   echo "jobname        : $JOBNAME"
   echo "numcpus        : $NUMCPUS"
   echo "numtasks       : $NUMTASKS"
   echo "numnodes       : $NUMNODES"
   echo "tasks_per_node : $NUMTASKS_PERNODE"
   echo " "

   # KDR; debugging differences between 2019-09-30-21600 
   # before and after cheyenne July down time.
   # Mick says the old MPT, which was used to build and run the old filter,
   # was mpt/2.21.
   # 2022-2-9; We're using a new filter built with system defaults, so don't swap modules.
   # module swap mpt/2.21
   echo "Modules used for this assimilation:"
   module list
   echo "-----------------------------------"

else if ($?LSB_HOSTS) then

   # LSF environment variables:
   # env | grep LS | grep -v LS_COLORS | sort

   setenv ORIGINALDIR $LS_SUBCWD
   setenv     JOBNAME $LSB_OUTPUTFILE:ar
   setenv       JOBID $LSB_JOBID
   setenv     MYQUEUE $LSB_QUEUE
   setenv   NODENAMES ${LSB_HOSTS}
   setenv MP_DEBUG_NOTIMEOUT yes
   setenv LAUNCHCMD mpirun.lsf
#  untested method for determining NUMTASKS_PERNODE with LSF
#  setenv NUMTASKS_PERNODE \
#     `echo $LSB_SUB_RES_REQ | sed -ne '/ptile/s#.*\[ptile=\([0-9][0-9]*\)]#\1#p'`

endif

# ==============================================================================
# Block 0: Set command environment
# ==============================================================================
# This block is an attempt to localize all the machine-specific
# changes to this script such that the same script can be used
# on multiple platforms. This will help us maintain the script.

echo "`date` -- BEGIN CAM_ASSIMILATE"

set nonomatch      # suppress "rm" warnings if wildcard does not match anything

setenv CASEROOT $1

# CESM uses C indexing on loops; cycle = [0,....,$DATA_ASSIMILATION_CYCLES - 1]
# "Fix" that here, so the rest of the script isn't confusing.

@ cycle = $2 + 1

cd ${CASEROOT}

# FIXME; variables in data_scripts.csh are redundant with the setenvs that follow.
# Clean it up some day.
source ./data_scripts.csh

setenv scomp                     `./xmlquery COMP_ATM      --value`
setenv CASE                      `./xmlquery CASE          --value`
setenv ensemble_size             `./xmlquery NINST_ATM     --value`
setenv CAM_DYCORE                `./xmlquery CAM_DYCORE    --value`
setenv EXEROOT                   `./xmlquery EXEROOT       --value`
setenv RUNDIR                    `./xmlquery RUNDIR        --value`
setenv archive                   `./xmlquery DOUT_S_ROOT   --value`
setenv TOTALPES                  `./xmlquery TOTALPES      --value`
setenv CONT_RUN                  `./xmlquery CONTINUE_RUN  --value`
setenv CHECK_TIMING              `./xmlquery CHECK_TIMING  --value`
setenv DATA_ASSIMILATION_CYCLES  `./xmlquery DATA_ASSIMILATION_CYCLES --value`

echo "============================================================="
echo "Brian's suggestions of checks:"
echo "which mpiexec_mpt"
which mpiexec_mpt
echo "mpif90 -show"
mpif90 -show | sed -e 's# -#\n     -#g'
echo "ldd ./filter" 
ldd ${EXEROOT}/filter 
echo "End of module checking section"
echo "============================================================="

# Switch CESM's timer script off for the rest of the forecasts of this job.
# The timer takes a significant amount of time, which grows by ~15 s
# for each cycle.  This can double the cycle time in a 2 week job.

./xmlchange CHECK_TIMING=FALSE

cd ${RUNDIR}

# A switch to save all the inflation files
setenv save_all_inf TRUE

# This may be needed before the short-term archiver has been run.
if (! -d ${archive}/esp/hist) mkdir -p ${archive}/esp/hist

# If they exist, mean and sd will always be saved.
# A switch to signal how often to save the stages' ensemble members.
#     valid values are:  NONE, RESTART_TIMES, ALL
setenv save_stages_freq RESTART_TIMES

# This next line ultimately specifies the location of the observation files.
# set BASEOBSDIR = /glade/p/cisl/dares/Observations/NCEP+ACARS+GPS+AIRS/Thinned_x9x10
set BASEOBSDIR = /glade/p/cisl/dares/Observations/NCEP+ACARS+GPS+AIRS/ThinAIRS_9x10-C2+C1

# suppress "rm" warnings if wildcard does not match anything
set nonomatch

# Make sure that this script is using standard system commands
# instead of aliases defined by the user.
# If the standard commands are not in the location listed below,
# change the 'set' commands to use them.
# The VERBOSE options are useful for debugging, but are optional because
# some systems don't like the -v option to any of the following.

set   MOVE = '/usr/bin/mv -v'
set   COPY = '/usr/bin/cp -v --preserve=timestamps'
set   LINK = '/usr/bin/ln -s'
set   LIST = '/usr/bin/ls '
set REMOVE = '/usr/bin/rm -r'

# ==============================================================================
# Block 1: Determine time of current model state from file name of member 1
# These are of the form "${CASE}.cam_${ensemble_member}.i.2000-01-06-00000.nc"
# ==============================================================================

# Piping stuff through 'bc' strips off any preceeding zeros.

set FILE = `head -n 1 rpointer.atm_0001`
set FILE = $FILE:r
set ATM_DATE_EXT = $FILE:e
set ATM_DATE     = `echo $FILE:e | sed -e "s#-# #g"`
set ATM_YEAR     = `echo $ATM_DATE[1] | bc`
set ATM_MONTH    = `echo $ATM_DATE[2] | bc`
set ATM_DAY      = `echo $ATM_DATE[3] | bc`
set ATM_SECONDS  = `echo $ATM_DATE[4] | bc`
set ATM_HOUR     = `echo $ATM_DATE[4] / 3600 | bc`

echo "valid time of model is $ATM_YEAR $ATM_MONTH $ATM_DAY $ATM_SECONDS (seconds)"
echo "valid time of model is $ATM_YEAR $ATM_MONTH $ATM_DAY $ATM_HOUR (hours)"

# Move the hidden restart set back into $rundir so that it is processed properly.

${LIST} -d ../Hide*
if ($status == 0) then
   echo 'Moving hidden restarts into the run directory so they can be used or purged.'
   ${MOVE} ../Hide*/* .
   rmdir   ../Hide*
endif

# We need to know the names of the current cesm.log files - one log file is created
# by each CESM model advance.

# How long does this listing take?
date --rfc-3339=ns
set log_list = `${LIST} -t cesm.log.*`

echo "most recent log is $log_list[1]"
echo "oldest      log is $log_list[$#log_list]"
echo "entire log list is $log_list"
echo " "
date --rfc-3339=ns

# ==============================================================================
# Block 2: Populate a run-time directory with the input needed to run DART.
# ==============================================================================

echo "`date` -- BEGIN COPY BLOCK"

# Put a pared down copy (no comments) of input.nml in this assimilate_cam directory.
# The contents may change from one cycle to the next, so always start from
# the known configuration in the CASEROOT directory.

if (  -e   ${CASEROOT}/input.nml ) then

   sed -e "/#/d;/^\!/d;/^[ ]*\!/d" \
       -e '1,1i\WARNING: Changes to this file will be ignored. \n Edit \$CASEROOT/input.nml instead.\n\n\n' \
       ${CASEROOT}/input.nml >! input.nml  || exit 10
else
   echo "ERROR ... DART required file ${CASEROOT}/input.nml not found ... ERROR"
   echo "ERROR ... DART required file ${CASEROOT}/input.nml not found ... ERROR"
   exit 11
endif

echo "`date` -- END COPY BLOCK"

# If possible, use the round-robin approach to deal out the tasks.
# This facilitates using multiple nodes for the simultaneous I/O operations.

if ($?NUMTASKS_PERNODE) then
   if ($#NUMTASKS_PERNODE > 0) then
      ${MOVE} input.nml input.nml.$$ || exit 20
      sed -e "s#layout.*#layout = 2#" \
          -e "s#tasks_per_node.*#tasks_per_node = $NUMTASKS_PERNODE#" \
          input.nml.$$ >! input.nml || exit 21
      ${REMOVE} -f input.nml.$$
   endif
endif

# ==============================================================================
# Block 3: Identify requested output stages, warn about redundant output.
# ==============================================================================

set MYSTRING = `grep stages_to_write input.nml`
set MYSTRING = (`echo $MYSTRING | sed -e "s#[=,'\.]# #g"`)
set STAGE_input     = FALSE
set STAGE_forecast  = FALSE
set STAGE_preassim  = FALSE
set STAGE_postassim = FALSE
set STAGE_analysis  = FALSE
set STAGE_output    = FALSE

# Assemble lists of stages to write out, which are not the 'output' stage.

set stages_except_output = "{"
@ stage = 2
while ($stage <= $#MYSTRING)
   if ($MYSTRING[$stage] == 'input') then
      set STAGE_input = TRUE
      if ($stage > 2) set stages_except_output = "${stages_except_output},"
      set stages_except_output = "${stages_except_output}input"
   endif
   if ($MYSTRING[$stage] == 'forecast') then
      set STAGE_forecast = TRUE
      if ($stage > 2) set stages_except_output = "${stages_except_output},"
      set stages_except_output = "${stages_except_output}forecast"
   endif
   if ($MYSTRING[$stage] == 'preassim') then
      set STAGE_preassim = TRUE
      if ($stage > 2) set stages_except_output = "${stages_except_output},"
      set stages_except_output = "${stages_except_output}preassim"
   endif
   if ($MYSTRING[$stage] == 'postassim') then
      set STAGE_postassim = TRUE
      if ($stage > 2) set stages_except_output = "${stages_except_output},"
      set stages_except_output = "${stages_except_output}postassim"
   endif
   if ($MYSTRING[$stage] == 'analysis') then
      set STAGE_analysis = TRUE
      if ($stage > 2) set stages_except_output = "${stages_except_output},"
      set stages_except_output = "${stages_except_output}analysis"
   endif
   if ($stage == $#MYSTRING) then
      set stages_all = "${stages_except_output}"
      if ($MYSTRING[$stage] == 'output') then
         set STAGE_output = TRUE
         set stages_all = "${stages_all},output"
      endif
   endif
   @ stage++
end

# Add the closing }
set stages_all = "${stages_all}}"
set stages_except_output = "${stages_except_output}}"

# Checking
echo "stages_except_output = $stages_except_output"
echo "stages_all = $stages_all"
if ($STAGE_output != TRUE) then
   echo "ERROR: assimilate.csh requires that input.nml:filter_nml:stages_to_write includes stage 'output'"
   exit 40
endif

# ==============================================================================
# Block 4: Preliminary clean up, which can run in the background.
# ==============================================================================
# CESM2_0's new archiver has a mechanism for removing restart file sets,
# which we don't need, but it runs only after the (multicycle) job finishes.
# We'd like to remove unneeded restarts as the job progresses, allowing more
# cycles to run before needing to stop to archive data.  So clean them out of
# RUNDIR, and st_archive will never have to deal with them.
# ------------------------------------------------------------------------------

# For safety, leave the most recent *2* restart sets in place.
# Prevents catastrophe if the last restart set is partially written before a crash.
# Add 1 more because the restart set used to start this will be counted:
# there will be 3 restarts when there are only 2 cesm.log files,
# which caused all the files to be deleted.

if ($#log_list >= 3) then

   # List of potential restart sets to remove. The coupler restart files
   # may or may not have an 'instance' string in them, depending on whether
   # or not you are using the multi-driver or not, so we must check for both.

   echo "Gathering re_list at " `date --rfc-3339=ns`
   set re_list = `${LIST} -t *cpl.r.*`
   if ($#re_list == 0) set re_list = `${LIST} -t *cpl_0001.r.*`
   echo "finished gathering re_list at " `date --rfc-3339=ns`

   if ($#re_list < 3) then
      echo "ERROR: Too many cesm.log files ($#log_list) for the $#re_list restart sets."
      echo "       Clean out the cesm.log files from failed cycles."
      exit 50
   endif

   # Find the date of the oldest restart set from filenames like:
   # setup_test.cpl_0001.r.2016-12-11-21600.nc   ... or ...
   # setup_test.cpl.r.2016-12-11-21600.nc.gz
   #
   # Grab the root of the filename (removes the .nc 'extension')
   # and then the extension is the bit we need.
   # Want the YYYY-MM-DD-SSSSS part as well as 'DD-SSSSS'

   set FILE = $re_list[3]
   set FILE = $FILE:r
   if ($FILE:e == 'nc') set FILE = $FILE:r
   set rm_date = $FILE:e

   set RM_DATE_PARTS = `echo $rm_date | sed -e "s#-# #g"`
   set day_o_month = $RM_DATE_PARTS[3]
   set sec_o_day   = $RM_DATE_PARTS[4]
   set day_time    = ${day_o_month}-${sec_o_day}
   
   # Decide whether to purge restart files at this date and time.
   set save_rest_freq = Monday
   
   set purge = 'true'
   # Learn whether save_rest_freq a string or a number.
   # Character strings must be tested outside of the 'if' statement.
   echo $save_rest_freq | grep '[a-z]'
   if ($status == 0) then
      set purge_date = $RM_DATE_PARTS[1]-$RM_DATE_PARTS[2]-$RM_DATE_PARTS[3]
      set weekday = `date --date="$purge_date" +%A`
      if ($weekday == $save_rest_freq) set purge = 'false'

   # Numbers can be tested inside the 'if' statement.
   else if (`echo $save_rest_freq | grep '[0-9]'`) then
      if (${day_o_month} % ${save_rest_freq} == 0) set purge = 'false'

   endif


   # Identify log files to be removed or moved.
   # [3] means the 3rd oldest restart set is being (re)moved.
   set rm_log = `echo $log_list[3] | sed -e "s/\./ /g;"`
   set rm_slot = $#rm_log
   if ($rm_log[$#rm_log] == 'gz') @ rm_slot--
   echo 'oldest restart set is from job tagged $rm_log['$rm_slot']='$rm_log[$rm_slot]

   # This first half of the statement removes unwanted restarts.
   # The 'else' block preserves the restarts in the archive directory.

   if ( $sec_o_day !~ '00000' || \
       ($sec_o_day =~ '00000' && $purge == 'true') ) then

      # Optionally save inflation restarts, even if it's not a 'save restart' time.
      if ($save_all_inf =~ TRUE) ${MOVE} ${CASE}*inf*${day_time}*  ${archive}/esp/hist

      # Remove intermediate member restarts,
      # but not DART means, sd, obs_seq, inflation restarts output.
      # Note that *cpl.h[ar]* are retained, and any h#, #>0.

      echo "Removing unneeded restart file set (DD_SSSSS ${day_time}) from RUNDIR: "
      # It's OK to remove rh# (all components) because only the last date is needed,
      # while this is the 3rd to last date.  Also, if the history output is averaged, 
      # the averaging period is (should be) the length of the forecast, 
      # so there will be no running averages stored in the rh# files.
      # But the h0 files need to be kept if any user specified fields are in them.
      # They will be handled by repack_st_archive.csh and/or purge.csh later.
      echo "     ${CASE}"'*.{r,rs,rs1,rh*}.*'"${day_time}"
      ${REMOVE}  ${CASE}*.{r,rs,rs1,rh[0-9]}.*${day_time}* &
      # echo "     ${CASE}"'*.{r,rs,rs1,rh0,h0}.*'"${day_time}"
      # ${REMOVE}  ${CASE}*.{r,rs,rs1,rh0,h0}.*${day_time}* &

      # Handle .i. separately to avoid sweeping up .i.${scomp}_{in,out}put_{mean,sd,...} files.
      echo "     ${CASE}"'*.i.[0-9]*'"${day_time}"
      ${REMOVE} ${CASE}*.i.[0-9]*${day_time}*  &

      if ($save_stages_freq =~ NONE || $save_stages_freq =~ RESTART_TIMES) then
         # 'output' will have been renamed by the time the purging happens.
         echo "     ${CASE}"'*'[0-9].e.${stages_except_output}'*'${day_time}
         ${REMOVE}  ${CASE}.*[0-9].e.${stages_except_output}*${day_time}* &
      endif
   else

      echo "Preserving (compressed) restart file set (DD_SSSSS ${day_time})"

      # Optionally COPY inflation restarts to the same place as the other inflation restarts.
      if ($save_all_inf =~ TRUE) then
          ${COPY} ${CASE}*inf*${day_time}*  ${archive}/esp/hist &
      endif

      # Optionally REMOVE stages' ensemble members (not means and sds).
      if ($save_stages_freq =~ NONE ) then
         echo "Removing unneeded stages' ensemble members (DD_SSSSS ${day_time}) from RUNDIR: "
         echo "     ${CASE}"'*'[0-9].${stages_except_output}'*'${day_time}
         ${REMOVE}  ${CASE}.*[0-9].${stages_except_output}*${day_time}* &
      endif

      wait

      # The list of components determines which restarts are compressed by this call.
      # List the large files first (more efficient and reliable).
      # There is another call farther down to compress the DART files every cycle.
      echo "compress.csh started at `date`"
      ${CASEROOT}/compress.csh gzip ${rm_date} "clm2 cpl cam cice" "$stages_all"
      if ($status != 0) then
         echo "compress.csh failed at `date`"
         exit 55
      endif
      echo "compress.csh finished at `date`"

      # Save the restart set to archive/rest/$datename,
      # where it will be safe from removes of $component/rest.
      # There is an implicit assumption that some sort of inflation will be used.

      set save_root = ${archive}/rest/${rm_date}
      if (! -d $save_root) then
         mkdir -p $save_root
         (${MOVE} ${CASE}*.{r,rs,rs1,rh[0-9]}.*${day_time}*  $save_root || exit 60) &
         (${MOVE} ${CASE}*.i.[0-9]*${day_time}*             $save_root || exit 61) &
         (${COPY} *output*inf*${day_time}*                  $save_root || exit 62) &
         (${MOVE} *0001*${rm_log[$rm_slot]}*                $save_root || exit 63) &
         (${MOVE} cesm*${rm_log[$rm_slot]}*                 $save_root || exit 64) &
      else
         echo "WARNING: $save_root already exists.  Did st_archive make it?"
#          exit 65
      endif
   endif

   # Remove log files: *YYMMDD-HHMMSS*, except not da.log files, after moves and copies are done.
   wait
   ${REMOVE}  [^d]*${rm_log[$rm_slot]}*  &

   # I'd like to remove the CAM .r. files, since we always use the .i. files to do a hybrid start,
   # but apparently CESM needs them to be there, even though it doesn't read fields from them.
   # ${REMOVE}  ${CASE}.cam*.r.*${day_time}.nc &

endif

# ==============================================================================
# Block 5: Get observation sequence file ... or die right away.
# The observation file names have a time that matches the stopping time of CAM.
#
# Make sure the file name structure matches the obs you will be using.
# PERFECT model obs output appends .perfect to the filenames
# ==============================================================================

set YYYYMM = `printf %04d%02d ${ATM_YEAR} ${ATM_MONTH}`

if (! -d ${BASEOBSDIR}/${YYYYMM}_6H_CESM) then
   echo "CESM+DART requires 6 hourly obs_seq files in directories of the form YYYYMM_6H_CESM"
   echo "The directory ${BASEOBSDIR}/${YYYYMM}_6H_CESM is not found.  Exiting"
   exit 70
endif

set OBSFNAME = `printf obs_seq.%04d-%02d-%02d-%05d ${ATM_YEAR} ${ATM_MONTH} ${ATM_DAY} ${ATM_SECONDS}`

set OBS_FILE = ${BASEOBSDIR}/${YYYYMM}_6H_CESM/${OBSFNAME}
echo "OBS_FILE = $OBS_FILE"

${REMOVE} obs_seq.out
if (  -e ${OBS_FILE} ) then
   ${LINK} ${OBS_FILE} obs_seq.out || exit 80
else
   echo "ERROR ... no observation file ${OBS_FILE}"
   echo "ERROR ... no observation file ${OBS_FILE}"
   exit 81
endif

# ==============================================================================
# Block 6: DART INFLATION
# This block is only relevant if 'inflation' is turned on AND
# inflation values change through time:
# filter_nml
#    inf_flavor(:)  = 2  (or 3 (or 4 for posterior))
#    inf_initial_from_restart    = .TRUE.
#    inf_sd_initial_from_restart = .TRUE.
#
# This block stages the files that contain the inflation values.
# The inflation files are essentially duplicates of the DART model state,
# which have names in the CESM style, something like
#    ${case}.dart.rh.${scomp}_output_priorinf_{mean,sd}.YYYY-MM-DD-SSSSS.nc
# The strategy is to use the latest such files in ${RUNDIR}.
# If those don't exist at the start of an assimilation,
# this block creates them with 'fill_inflation_restart'.
# If they don't exist AFTER the first cycle, the script will exit
# because they should have been available from a previous cycle.
# The script does NOT check the model date of the files for consistency
# with the current forecast time, so check that the inflation mean
# files are evolving as expected.
#
# CESM's st_archive should archive the inflation restart files
# like any other "restart history" (.rh.) files; copying the latest files
# to the archive directory, and moving all of the older ones.
# ==============================================================================

# If we need to run fill_inflation_restart, CAM:static_init_model()
# always needs a caminput.nc and a cam_phis.nc for geometry information, etc.

set MYSTRING = `grep cam_template_filename input.nml`
set MYSTRING = `echo $MYSTRING | sed -e "s#[=,']# #g"`
set CAMINPUT = $MYSTRING[2]
${REMOVE} ${CAMINPUT}
${LINK} ${CASE}.cam_0001.i.${ATM_DATE_EXT}.nc ${CAMINPUT} || exit 90

# All of the .h0. files contain the same PHIS field, so we can link to any of them.
set MYSTRING = `grep cam_phis_filename input.nml`
set MYSTRING = `echo $MYSTRING | sed -e "s#[=,']# #g"`
set CAM_PHIS = $MYSTRING[2]
# Avoid doing LISTs in rundir; only search for an h0 file if cam_phis.nc doesn't exist.
if (! -f ${CAM_PHIS}) then
   # -r puts the file with the latest date first in the list.
   # It won't be purged during this cycle by the cleanup (above).
   set hists = `${LIST} -r ${CASE}.cam_0001.h0.*.nc`
   ${COPY} $hists ${CAM_PHIS} || exit 100
endif

# Now, actually check the inflation settings

set  MYSTRING = `grep inf_flavor input.nml`
set  MYSTRING = `echo $MYSTRING | sed -e "s#[=,'\.]# #g"`
set  PRIOR_INF = $MYSTRING[2]
set  POSTE_INF = $MYSTRING[3]

set  MYSTRING = `grep inf_initial_from_restart input.nml`
set  MYSTRING = `echo $MYSTRING | sed -e "s#[=,'\.]# #g"`

# If no inflation is requested, the inflation restart source is ignored

if ( $PRIOR_INF == 0 ) then
   set  PRIOR_INFLATION_FROM_RESTART = ignored
   set  USING_PRIOR_INFLATION = false
else
   set  PRIOR_INFLATION_FROM_RESTART = `echo $MYSTRING[2] | tr '[:upper:]' '[:lower:]'`
   set  USING_PRIOR_INFLATION = true
endif

if ( $POSTE_INF == 0 ) then
   set  POSTE_INFLATION_FROM_RESTART = ignored
   set  USING_POSTE_INFLATION = false
else
   set  POSTE_INFLATION_FROM_RESTART = `echo $MYSTRING[3] | tr '[:upper:]' '[:lower:]'`
   set  USING_POSTE_INFLATION = true
endif

if ($USING_PRIOR_INFLATION == false ) then
   set stages_requested = 0
   if ( $STAGE_input    == TRUE ) @ stages_requested++
   if ( $STAGE_forecast == TRUE ) @ stages_requested++
   if ( $STAGE_preassim == TRUE ) @ stages_requested++
   if ( $stages_requested > 1 ) then
      echo " "
      echo "WARNING ! ! Redundant output is requested at multiple stages before assimilation."
      echo "            Stages 'input' and 'forecast' are always redundant."
      echo "            Prior inflation is OFF, so stage 'preassim' is also redundant. "
      echo "            We recommend requesting just 'preassim'."
      echo " "
   endif
endif

if ($USING_POSTE_INFLATION == false ) then
   set stages_requested = 0
   if ( $STAGE_postassim == TRUE ) @ stages_requested++
   if ( $STAGE_analysis  == TRUE ) @ stages_requested++
   if ( $STAGE_output    == TRUE ) @ stages_requested++
   if ( $stages_requested > 1 ) then
      echo " "
      echo "WARNING ! ! Redundant output is requested at multiple stages after assimilation."
      echo "            Stages 'output' and 'analysis' are always redundant."
      echo "            Posterior inflation is OFF, so stage 'postassim' is also redundant. "
      echo "            We recommend requesting just 'output'."
      echo " "
   endif
endif

# IFF we want PRIOR inflation:

if ($USING_PRIOR_INFLATION == true) then
   if ($PRIOR_INFLATION_FROM_RESTART == false) then

      echo "inf_flavor(1) = $PRIOR_INF, using namelist values."

   else
      # Look for the output from the previous assimilation (or fill_inflation_restart)
      # If inflation files exists, use them as input for this assimilation
      # TODO: This should probably look for a date, instead of relying on the files
      #       we want being the youngest in the directory.
      echo "Gathering inflation names at " `date --rfc-3339=ns`
      (${LIST} -rt1 *.dart.rh.${scomp}_output_priorinf_mean* | tail -n 1 >! latestfile) > & /dev/null
      (${LIST} -rt1 *.dart.rh.${scomp}_output_priorinf_sd*   | tail -n 1 >> latestfile) > & /dev/null
      echo "finished gathering inflation names at " `date --rfc-3339=ns`
      set nfiles = `cat latestfile | wc -l`

      if ( $nfiles > 0 ) then

         set latest_mean = `head -n 1 latestfile`
         set latest_sd   = `tail -n 1 latestfile`
         # Need to COPY instead of link because of short-term archiver and disk management.
         ${COPY} $latest_mean input_priorinf_mean.nc
         ${COPY} $latest_sd   input_priorinf_sd.nc

      else if ($CONT_RUN == FALSE) then

         # It's the first assimilation; try to find some inflation restart files
         # or make them using fill_inflation_restart.
         # Fill_inflation_restart needs caminput.nc and cam_phis.nc for static_model_init,
         # so this staging is done in assimilate.csh (after a forecast) instead of stage_cesm_files.

         if (-x ${EXEROOT}/fill_inflation_restart) then

            ${EXEROOT}/fill_inflation_restart

         else
            echo "ERROR: Requested PRIOR inflation restart for the first cycle."
            echo "       There are no existing inflation files available "
            echo "       and ${EXEROOT}/fill_inflation_restart is missing."
            echo "EXITING"
            exit 112
         endif

      else
         echo "ERROR: Requested PRIOR inflation restart, "
         echo '       but files *.dart.rh.${scomp}_output_priorinf_* do not exist in the ${RUNDIR}.'
         echo '       If you are changing from cam_no_assimilate.csh to assimilate.csh,'
         echo '       you might be able to continue by changing CONTINUE_RUN = FALSE for this cycle,'
         echo '       and restaging the initial ensemble.'
         ${LIST} -l *inf*
         echo "EXITING"
         exit 115
      endif
   endif
else
   echo "Prior Inflation not requested for this assimilation."
endif

# POSTERIOR: We look for the 'newest' and use it - IFF we need it.

if ($USING_POSTE_INFLATION == true) then
   if ($POSTE_INFLATION_FROM_RESTART == false) then

      # we are not using an existing inflation file.
      echo "inf_flavor(2) = $POSTE_INF, using namelist values."

   else
      # Look for the output from the previous assimilation (or fill_inflation_restart).
      # (The only stage after posterior inflation.)
      (${LIST} -rt1 *.dart.rh.${scomp}_output_postinf_mean* | tail -n 1 >! latestfile) > & /dev/null
      (${LIST} -rt1 *.dart.rh.${scomp}_output_postinf_sd*   | tail -n 1 >> latestfile) > & /dev/null
      set nfiles = `cat latestfile | wc -l`

      # If one exists, use it as input for this assimilation
      if ( $nfiles > 0 ) then

         set latest_mean = `head -n 1 latestfile`
         set latest_sd   = `tail -n 1 latestfile`
         ${REMOVE} input_postinf*.nc
         ${LINK} $latest_mean input_postinf_mean.nc || exit 120
         ${LINK} $latest_sd   input_postinf_sd.nc   || exit 121

      else if ($CONT_RUN == FALSE) then
         # It's the first assimilation; try to find some inflation restart files
         # or make them using fill_inflation_restart.
         # Fill_inflation_restart needs caminput.nc and cam_phis.nc for static_model_init,
         # so this staging is done in assimilate.csh (after a forecast) instead of stage_cesm_files.

         if (-x ${EXEROOT}/fill_inflation_restart) then
            ${EXEROOT}/fill_inflation_restart
            ${MOVE} prior_inflation_mean.nc input_postinf_mean.nc || exit 125
            ${MOVE} prior_inflation_sd.nc   input_postinf_sd.nc   || exit 126

         else
            echo "ERROR: Requested POSTERIOR inflation restart for the first cycle."
            echo "       There are no existing inflation files available "
            echo "       and ${EXEROOT}/fill_inflation_restart is missing."
            echo "EXITING"
            exit 127
         endif

      else
         echo "ERROR: Requested POSTERIOR inflation restart, "
         echo '       but files *.dart.rh.${scomp}_output_postinf_* do not exist in the ${RUNDIR}.'
         ${LIST} -l *inf*
         echo "EXITING"
         exit 128
      endif
   endif
else
   echo "Posterior Inflation not requested for this assimilation."
endif

# ==============================================================================
# Block 7: Actually run the assimilation.
#
# DART namelist settings required:
# &filter_nml
#    adv_ens_command         = "no_CESM_advance_script",
#    obs_sequence_in_name    = 'obs_seq.out'
#    obs_sequence_out_name   = 'obs_seq.final'
#    single_file_in          = .false.,
#    single_file_out         = .false.,
#    stages_to_write         = stages you want + ,'output'
#    input_state_file_list   = 'cam_init_files'
#    output_state_file_list  = 'cam_init_files',
#
# WARNING: the default mode of this script assumes that
#          input_state_file_list = output_state_file_list, so that
#          the CAM initial files used as input to filter will be overwritten.
#          The input model states can be preserved by requesting that stage
#          'forecast' be output.
#
# ==============================================================================

# In the default mode of CAM assimilations, filter gets the model state(s)
# from CAM initial files.  This section puts the names of those files into a text file.
# The name of the text file is provided to filter in filter_nml:input_state_file_list.

# NOTE:
# If the files in input_state_file_list are CESM initial files (all vars and
# all meta data), then they will end up with a different structure than
# the non-'output', stage output written by filter ('preassim', 'postassim', etc.).
# This can be prevented (at the cost of more disk space) by copying
# the CESM format initial files into the names filter will use for preassim, etc.:
#    > cp $case.cam_0001.i.$date.nc  preassim_member_0001.nc.
#    > ... for all members
# Filter will replace the state variables in preassim_member* with updated versions,
# but leave the other variables and all metadata unchanged.

# If filter will create an ensemble from a single state,
#    filter_nml: perturb_from_single_instance = .true.
# it's fine (and convenient) to put the whole list of files in input_state_file_list.
# Filter will just use the first as the base to perturb.

set line = `grep input_state_file_list input.nml | sed -e "s#[=,'\.]# #g"`
set input_file_list_name = $line[2]

# If the file names in $output_state_file_list = names in $input_state_file_list,
# then the restart file contents will be overwritten with the states updated by DART.

set line = `grep output_state_file_list input.nml | sed -e "s#[=,'\.]# #g"`
set output_file_list_name = $line[2]

if ($input_file_list_name != $output_file_list_name) then
   echo "ERROR: assimilate.csh requires that input_file_list = output_file_list"
   echo "       You can probably find the data you want in stage 'forecast'."
   echo "       If you truly require separate copies of CAM's initial files"
   echo "       before and after the assimilation, see revision 12603, and note that"
   echo "       it requires changing the linking to cam_initial_####.nc, below."
   exit 130
endif

echo "Gathering CAM initial names at " `date --rfc-3339=ns`
${LIST} -1 ${CASE}.cam_[0-9][0-9][0-9][0-9].i.${ATM_DATE_EXT}.nc >! $input_file_list_name
echo "finished gathering CAM initial names at " `date --rfc-3339=ns`

echo "`date` -- BEGIN FILTER"
# 2 lines added for Ben to debug cycle slowing and job timing out.
# echo "before" | logger -t raederdebug
# ps auxfw | logger -t raederdebug


${LAUNCHCMD} ${EXEROOT}/filter || exit 140

# 4 lines added for Ben to debug cycle slowing and job timing out.
# echo "after" | logger -t raederdebug
# ps auxfw | logger -t raederdebug
# sleep 5
# ps auxfw | logger -t raederdebug

echo "`date` -- END FILTER"

# ==============================================================================
# Block 8: Rename the output using the CESM file-naming convention.
# ==============================================================================

# If output_state_file_list is filled with custom (CESM) filenames,
# then 'output' ensemble members will not appear with filter's default,
# hard-wired names.  But file types output_{mean,sd} will appear and be
# renamed here.
#
# We don't know the exact set of files which will be written,
# so loop over all possibilities: use LIST in the foreach.
# LIST will expand the variables and wildcards, only existing files will be
# in the foreach loop. (If the input.nml has num_output_state_members = 0,
# there will be no output_member_xxxx.nc even though the 'output' stage
# may be requested - for the mean and sd) 
#
# Handle files with instance numbers first.
#    split off the .nc
#    separate the pieces of the remainder
#    grab all but the trailing 'member' and #### parts.
#    and join them back together

echo "`date` -- BEGIN FILE RENAMING"

# The short-term archiver archives files depending on pieces of their names.
# '_####.i.' files are CESM initial files.
# '.dart.i.' files are ensemble statistics (mean, sd) of just the state variables 
#            in the initial files.
# '.e.'      files are from the 'external system processing (ESP)', e.g. DART.

echo "Gathering stages members names at " `date --rfc-3339=ns`
foreach FILE (`${LIST} ${stages_all}_member_*.nc`)
   echo "started the loop stages members names at " `date --rfc-3339=ns`

   set parts = `echo $FILE | sed -e "s#\.# #g"`
   set list = `echo $parts[1]  | sed -e "s#_# #g"`
   @ last = $#list - 2
   set dart_file = `echo $list[1-$last] | sed -e "s# #_#g"`

   # DART 'output_member_****.nc' files are actually linked to cam input files

   set type = "e"
   echo $FILE | grep "put"
   if ($status == 0) set type = "i"

   ${MOVE} $FILE \
       ${CASE}.${scomp}_$list[$#list].${type}.${dart_file}.${ATM_DATE_EXT}.nc || exit 150
end

# Files without instance numbers need to have the scomp part of their names = "dart".
# This is because in st_archive, all files with  scomp = "cam"
# (= compname in env_archive.xml) will be st_archived using a pattern
# which has the instance number added onto it.  {mean,sd} files don't have 
# instance numbers, so they need to be archived by the "dart" section of env_archive.xml.
# But they still need to be different for each component, so include $scomp in the
# ".dart_file" part of the file name.  Somewhat awkward and inconsistent, but effective.

# Means and standard deviation files (except for inflation; 
# excluded by, e.g., mean right after stage).
echo "Gathering mean, sd names at " `date --rfc-3339=ns`
foreach FILE (`${LIST} ${stages_all}_{mean,sd}*.nc`)
   echo "started mean,sd loop names at " `date --rfc-3339=ns`

   set parts = `echo $FILE | sed -e "s#\.# #g"`
   set type = "e"
   echo $FILE | grep "put"
   if ($status == 0) set type = "i"

   ${MOVE} $FILE ${CASE}.dart.${type}.${scomp}_$parts[1].${ATM_DATE_EXT}.nc || exit 160
end

# Rename the observation file and run-time output

${MOVE} obs_seq.final ${CASE}.dart.e.${scomp}_obs_seq_final.${ATM_DATE_EXT} || exit 170
${MOVE} dart_log.out                 ${scomp}_dart_log.${ATM_DATE_EXT}.out || exit 171

# Rename the inflation files and designate them as 'rh' files - which get
# reinstated in the run directory by the short-term archiver and are then
# available for the next assimilation cycle.
#
# Accommodate any possible inflation files.
# The .${scomp}_ part is needed by DART to distinguish
# between inflation files from separate components in coupled assims.

echo "Gathering inflation names at " `date --rfc-3339=ns`
foreach FILE (`${LIST} ${stages_all}_{prior,post}inf_*`)
   echo "started inflation loop names at " `date --rfc-3339=ns`

   set parts = `echo $FILE | sed -e "s#\.# #g"`
   ${MOVE} $FILE  ${CASE}.dart.rh.${scomp}_$parts[1].${ATM_DATE_EXT}.nc || exit 180

end

# Handle localization_diagnostics_files
set MYSTRING = `grep 'localization_diagnostics_file' input.nml`
set MYSTRING = `echo $MYSTRING | sed -e "s#[=,']# #g"`
set MYSTRING = `echo $MYSTRING | sed -e 's#"# #g'`
set loc_diag = $MYSTRING[2]
if (-f $loc_diag) then
   ${MOVE} $loc_diag  ${scomp}_${loc_diag}.dart.e.${ATM_DATE_EXT} || exit 190
endif

# Handle regression diagnostics
set MYSTRING = `grep 'reg_diagnostics_file' input.nml`
set MYSTRING = `echo $MYSTRING | sed -e "s#[=,']# #g"`
set MYSTRING = `echo $MYSTRING | sed -e 's#"# #g'`
set reg_diag = $MYSTRING[2]
if (-f $reg_diag) then
   ${MOVE} $reg_diag  ${scomp}_${reg_diag}.dart.e.${ATM_DATE_EXT} || exit 200
endif

# Then this script will need to feed the files in output_restart_list_file
# to the next model advance.
# This gets the .i. or .r. piece from the CESM format file name.
set line = `grep 0001 $output_file_list_name | sed -e "s#[\.]# #g"`
set l = 1
while ($l < $#line)
   if ($line[$l] =~ ${scomp}_0001) then
      @ l++
      set file_type = $line[$l]
      break
   endif
   @ l++
end

set member = 1
while ( ${member} <= ${ensemble_size} )

   set inst_string = `printf _%04d $member`
   set ATM_INITIAL_FILENAME = ${CASE}.${scomp}${inst_string}.${file_type}.${ATM_DATE_EXT}.nc

   ${REMOVE} ${scomp}_initial${inst_string}.nc
   ${LINK} $ATM_INITIAL_FILENAME ${scomp}_initial${inst_string}.nc || exit 210

   @ member++

end

echo "`date` -- END   FILE RENAMING"

if ($cycle == $DATA_ASSIMILATION_CYCLES) then
   echo "`date` -- BEGIN (NON-RESTART) ARCHIVING LOGIC"

   if ($#log_list >= 3) then

      # During the last cycle, hide the previous restart set
      # so that it's not archived, but is available.
      # (Coupled assimilations may need to keep multiple atmospheric
      #  cycles for each ocean cycle.)

      set FILE = $re_list[2]
      set FILE = $FILE:r
      if ($FILE:e == 'nc') set FILE = $FILE:r
      set hide_date = $FILE:e
      set HIDE_DATE_PARTS = `echo $hide_date | sed -e "s#-# #g"`
      set day_o_month = $HIDE_DATE_PARTS[3]
      set sec_o_day   = $HIDE_DATE_PARTS[4]
      set day_time    = ${day_o_month}-${sec_o_day}

      set hidedir = ../Hide_${day_time}
      mkdir $hidedir

      if ($save_all_inf =~ TRUE) then
         # Put the previous and current inflation restarts in the archive directory.
         # (to protect last from st_archive putting them in esp/hist)
         # 2020-1-18; Changed dir esp/rest -> esp/hist (one that exists!)  
         # while fixing wayward {mean,sd} files including inflation.
         ${MOVE}   ${CASE}*${stages_except_output}*inf*  ${archive}/esp/hist

         # Don't need previous inf restarts now, but want them to be archived later.
         # COPY instead of LINK because they'll be moved or used later.
         ${COPY}   ${CASE}*output*inf* ${archive}/esp/hist
      else
         # output*inf must be copied back because it needs to be in ${RUNDIR}
         # when st_archive runs to save the results of the following assim
         ${MOVE}   ${CASE}*inf*${day_time}*  $hidedir

         # Don't need previous inf restarts now, but want them to be archived later.
         ${COPY}   $hidedir/${CASE}*output*inf*${day_time}* .
      endif

      # Hide the CAM 'restart' files from the previous cycle (day_time) from the archiver.
      # ${MOVE}           ${CASE}*.{r,rs,rs1,rh0,h0,i}.*${day_time}*    $hidedir
      # We don't need/want to mess with the .rh files.
      # ${MOVE}           ${CASE}*.{r,rs,rs1,h0,i}.*${day_time}*    $hidedir
      # h0 should also not be hidden from the archiver (in case it has fields
      # besides CAM's PHIS).
      # Be more selective about file dates to move.
      # This old one moved previous months of .i. {mean,sd} files,
      # which prevented them from being archived to esp/hist
      # ${MOVE}           ${CASE}*.{r,rs,rs1,i}.*${day_time}*    $hidedir
      ${MOVE}           ${CASE}*.{r,rs,rs1}.*${hide_date}*    $hidedir
      # Don't move the ...dart.i.cam_*{mean,sd} files,
      # but move the ...{instance}.i. files.
      ${MOVE}           ${CASE}*[^t].i.*${hide_date}*    $hidedir

      # Move log files: *YYMMDD-HHMMSS.  [2] means the previous restart set is being moved.
      set rm_log = `echo $log_list[2] | sed -e "s/\./ /g;"`
      # -- (decrement by one) skips the gz at the end of the names.
      set rm_slot = $#rm_log
      if ($rm_log[$#rm_log] =~ gz) @ rm_slot--
      ${MOVE}  *$rm_log[$rm_slot]*  $hidedir

      # Also COPY the most recent log files ([1]) so that they will be returned to rundir
      # for the next job, and there will be the right number to trigger the archiving
      # of inflation files to esp/hist for prevmo-lastdayprevjob-64800
      # Implemented as part of fixing the wayward {mean,sd} files, incl. infl.
      set rm_log = `echo $log_list[1] | sed -e "s/\./ /g;"`
      # -- (decrement by one) skips the gz at the end of the names.
      set rm_slot = $#rm_log
      if ($rm_log[$#rm_log] =~ gz) @ rm_slot--
      ${COPY}  *$rm_log[$rm_slot]*  $hidedir
   endif

   # Restore CESM's timing logic for the first cycle of the next job.
   cd ${CASEROOT}
   ./xmlchange CHECK_TIMING=${CHECK_TIMING}
   cd ${RUNDIR}

   # Create a netCDF file which contains the names of DART inflation restart files.
   # This is needed in order to use the CESM st_archive mechanisms for keeping, 
   # in $RUNDIR, history files which are needed for restarts.
   # These special files must be labeled with '.rh.'.
   # St_archive looks in a .r. restart file for the names of these 'restart history' files.
   # DART's inflation files fit the definition of restart history files, so we put .rh. 
   # in their names.  Those file names must be found in a dart.r. file, which is created here.
   # Inflation restart file names for all components will be in this one restart file,
   # since the inflation restart files have the component names in them.

   set inf_list = `ls *output_{prior,post}inf_*.${ATM_DATE_EXT}.nc`
   set file_list = 'restart_hist = "./'$inf_list[1]\"
   set i = 2
   while ($i <= $#inf_list)
      set file_list = (${file_list}\, \"./$inf_list[$i]\")
      @ i++
   end
   cat << ___EndOfText >! inf_restart_list.cdl
       netcdf template {  // CDL file which ncgen will use to make a DART restart file
                          // containing just the names of the needed inflation restart files.
       dimensions:
            num_files = $#inf_list;
       variables:
            string  restart_hist(num_files);
            restart_hist:long_name = "DART restart history file names";
       data:
            $file_list;
       }
___EndOfText

   ncgen -k netCDF-4 -o ${CASE}.dart.r.${scomp}.${ATM_DATE_EXT}.nc inf_restart_list.cdl
   if ($status == 0) ${REMOVE} inf_restart_list.cdl

   echo "`date` -- END   ARCHIVING LOGIC"

#    # DEBUG st_archive by making a shadow copy of this directory.
#    module load nco
# 
#    if (-d ../run_shadow) ${REMOVE} -f ../run_shadow
#    mkdir ../run_shadow
# 
#    set comps = ('cam_'  'clm2_'  'mosart_' 'dart')
#    set vars  = ('nhfil' 'locfnh' 'locfnh'  'restart_hist')
# 
#    foreach f (`$LIST[1]`)
#       set gr_stat = 1
#       echo $f | grep '\.r\.'
#       if ($status == 0) then
#          set c = 1
#          while ($c <= $#comps) 
#             echo $f | grep $comps[$c]
#             if ($status == 0) then
#                echo "c = $c for $f"
# #                set echo verbose
#                set gr_stat = 0
#                ncks -O -v $vars[$c] $f ../run_shadow/$f
#                break
#             endif
#             @ c++
#          end
#       endif
#       if ($gr_stat == 1) then
#          ${LIST} -l $f >! ../run_shadow/$f
#       endif
#    end

endif

# ==============================================================================
# Compress the large coupler history files and DART files.
# ==============================================================================

# echo "STARTING: compressing coupler history files and DART files at `date`"
# ${CASEROOT}/compress.csh gzip $CASE $ATM_DATE_EXT $ensemble_size "hist dart" "$stages_all"
echo "STARTING: compressing coupler history files at `date`"
# Orig (mistaken, but ignored, last arg when "hist" is the component)
# ${CASEROOT}/compress.csh gzip $ATM_DATE_EXT "hist" "$stages_all"
# Adapt to new compress.csh that works with casper's ncpus limit
${CASEROOT}/compress.csh gzip $ATM_DATE_EXT "hist" "ha2x1d hr2x ha2x3h ha2x1h ha2x1hi"

if ($status != 0) then
   echo "ERROR: Compression of coupler history files and DART files failed at `date`"
   # Ensure the removal of unneeded restart sets and copy of obs_seq.final are finished.
   wait
   exit 250
endif

echo "FINISHED: compressing coupler history files and DART files at `date`"
echo "`date` -- END CAM_ASSIMILATE"

# Ensure the removal of unneeded restart sets and copy of obs_seq.final are finished.
wait

exit 0

