
#!/bin/csh -f

# This script defines data/arguments/parameters
# used by many non-CESM scripts in the workflow.

setenv  data_NINST            80
setenv  data_DART_src         /glade/u/home/raeder/DART/Manhattan_git
setenv  data_CASEROOT         /glade/work/raeder/Exp/Rean_BNRH_all_git/Rean_BNRH_all
setenv  data_CASE             Rean_BNRH_all
setenv  data_scratch          /glade/derecho/scratch/raeder/Rean_BNRH_all
setenv  data_campaign         /glade/campaign/cisl/dares/raeder/QCEFF
setenv  data_proj_space       ${data_campaign}/Rean_BNRH_all
setenv  data_CESM_python      /glade/work/raeder/Models/cesm2_1_m5.8/cime/scripts/lib/CIME 
setenv  data_DOUT_S_ROOT      /glade/derecho/scratch/raeder/Rean_BNRH_all/archive

# These assignments don't use START values from setup_... because data_scripts.csh
# is run later and must harvest the values at that time.
setenv CONTINUE_RUN `./xmlquery CONTINUE_RUN --value`
if ($CONTINUE_RUN == FALSE) then
   # YYYY-MM-DD  no seconds
   set START_DATE = `./xmlquery RUN_STARTDATE --value`

else if ($CONTINUE_RUN == TRUE) then
   # Get date from an rpointer file
   if (! -f ${data_scratch}/run/rpointer.atm_0001) then
      echo "CONTINUE_RUN = TRUE but "
      echo "${data_scratch}/run/rpointer.atm_0001 is missing.  Exiting"
      exit 19
   endif
   set FILE = `head -n 1 ${data_scratch}/run/rpointer.atm_0001`
   set START_DATE = $FILE:r:e

else
   echo "env_run.xml: CONTINUE_RUN must be FALSE or TRUE (case sensitive)"
   exit 30

endif
set parts = `echo $START_DATE | sed -e "s#-# #g"`
# Piping stuff through 'bc' strips off any preceeding zeros, so it can be used in a calculation.
# bc; the expression is not an assignment statement, so it is evaluated and printed to the output.
@ data_year  = `echo $parts[1] | bc`
@ data_month = `echo $parts[2] | bc`
@ data_day   = `echo $parts[3] | bc`
@ data_secs  = 0

