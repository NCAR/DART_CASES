#!/bin/tcsh

# A CESM case + DART has a large number of files because of the ensemble size.
# Many of them change with every job submission, but have uninformative changes.
# Here is a list of files archived for the CAM6+DART Reanalysis
# before each month of assimilation or change in the setup.
# This may not be comprehensive for your case, and probably names files which you don't need,
# especially if you're running a non-F-compset (CAM) assimilation.
# File types that have a file for each instance (member) are represented in this list
# by only the first 2 instances, so that they can be compared to see whether there are
# differences between instances.
# 
# This list uses csh wildcards (*, []) to shorten the list.

foreach ftype ( \
   '*.csv' \
   '.case.run' \
   '.env_mach_specific.csh' \
   '.env_mach_specific.sh' \
   'Buildconf/camconf/CESM_cppdefs' \
   'Buildconf/camconf/Filepath' \
   'Buildconf/camconf/atm_in' \
   'Buildconf/camconf/chem_mech.doc' \
   'Buildconf/camconf/chem_mech.in' \
   'Buildconf/camconf/config_cache.xml' \
   'Buildconf/camconf/docn_in' \
   'Buildconf/camconf/drv_flds_in' \
   'Buildconf/camconf/namelist' \
   'Buildconf/cice.input_data_list' \
   'Buildconf/ciceconf/ice_in' \
   'Buildconf/ciceconf/namelist_infile' \
   'Buildconf/clmconf/config_cache.xml' \
   'Buildconf/clmconf/drv_flds_in' \
   'Buildconf/clmconf/lnd_in' \
   'Buildconf/clmconf/namelist' \
   'Buildconf/cpl.input_data_list' \
   'Buildconf/cplconf/*_modelio.nml_000[12]' \
   'Buildconf/cplconf/drv_in' \
   'Buildconf/cplconf/namelist_infile' \
   'Buildconf/cplconf/seq_maps.rc' \
   'Buildconf/docn.input_data_list' \
   'Buildconf/docnconf/docn.streams.txt.prescribed_000[12]' \
   'Buildconf/docnconf/docn_in' \
   'Buildconf/docnconf/namelist_infile' \
   'Buildconf/mosart.input_data_list' \
   'Buildconf/mosartconf/mosart_in' \
   'Buildconf/mosartconf/namelist_infile' \
   'CaseDocs/*_in_000[12]' \
   'CaseDocs/*_modelio.nml_000[12]' \
   'CaseDocs/chem_mech.doc' \
   'CaseDocs/chem_mech.in' \
   'CaseDocs/docn.streams.txt.prescribed_000[12]' \
   'CaseDocs/drv_flds_in' \
   'CaseDocs/drv_in' \
   'CaseDocs/seq_maps.rc' \
   'CaseStatus' \
   'DART_config' \
   'Depends.intel' \
   'Macros.make' \
   'README.case' \
   'SourceMods/*/*' \
   'Tools/Makefile' \
   'Tools/check_lockedfiles' \
   'Tools/getTiming' \
   'Tools/mkDepends' \
   'Tools/mkSrcfiles' \
   'Tools/save_provenance' \
   'archive_metadata' \
   'assimilate.csh' \
   'case.build' \
   'case.cmpgen_namelists' \
   'case.qstatus' \
   'case.setup' \
   'case.st_archive' \
   'case.submit' \
   'check_case' \
   'check_input_data' \
   'compress.csh' \
   'data_scripts.csh' \
   'diags_rean.csh' \
   'env_archive.xml' \
   'env_batch.xml' \
   'env_build.xml' \
   'env_case.xml' \
   'env_mach_pes.xml' \
   'env_mach_specific.xml' \
   'env_run.xml' \
   'input.nml' \
   'launch_cf.sh' \
   'mv_to_campaign.csh' \
   'no_assimilate.csh' \
   'pelayout' \
   'pre_purge_check.csh' \
   'pre_submit.csh' \
   'purge.csh' \
   'repack_project.csh' \
   'repack_st_arch.csh' \
   'software_environment.txt' \
   'stage_cesm_files' \
   'stage_dart_files' \
   'submit_compress.csh' \
   'update_dart_namelists' \
   'user_nl_*_000[12]' \
   'user_nl_cpl' \
   )
   git add $ftype
end
exit

--------------
Other files archived in the Reanalysis github:
   'assimilate.csh.template' \
   '.gitignore' \
   'README.md' \
   'stage_cesm_files.template' \
Buildconf/cam.input_data_list.sorted
Buildconf/clm.input_data_list.sorted
CaseDocs/nml_in_0001.tar
CESM_instructions.txt
DART_instructions.txt
O2-xHost_def-envir_2022-2-23/filter
O2-xHost_def-envir_2022-2-23/minimal_build.out
O2-xHost_def-envir_2022-2-23/obs_def_mod.f90
O2-xHost_def-envir_2022-2-23/obs_kind_mod.f90
O2-xHost_obs_typeOK_2022-3-10/filter
O2-xHost_obs_typeOK_2022-3-10/obs_def_mod.f90
O2-xHost_obs_typeOK_2022-3-10/obs_kind_mod.f90
O2-xHost_obs_typeOK_2022-3-10/quickbuild.out
add_user_docn.streams.csh
all_but_submit
assim.csh.added
assim_no_debug4ben.csh
assim_post_filter.csh
backup_manually.casper
backup_manually.cpl
backup_manually.csh
bias_from_obs_seq_output.csh
call_mv_to_cs.csh
case.setup.MPI_COMM_MAX
caseroot_script_list
cesm_exe_220214-143228.tgz
chng_hybrid2branch.csh
compress.csh_proj2camp
compress_hist.csh
compress_joblogs.csh
compress_st-arch.csh
copy_atts.csh
diff.csh
env_archive.xml.original
env_batch.xml.original
env_build.xml.original
env_case.xml.original
env_mach_pes.xml.original
env_mach_specific.xml.original
env_run.xml.original
env_run_branch_first_2012.xml
env_run_branch_second_2012.xml
env_run_pre_branch.xml
finish_june
first_try
fix_yearly_atts.csh
hist_cleanup_2012-10
logs/run_environment.all_cycles_1_month
matlab_norm.csh
mover.csh
mover_proj2scratch.clm2_2011-3
mover_proj2scratch.cpl_2011-3
mover_proj2scratch.csh
mv_2011-05_to_CS_intera.atm.hist
mv_2011-05_to_CS_intera.esp.hist
mv_2011-05_to_CS_intera.logs
mv_2012-09-17-00000.csh
recreate_Reanalysis.notes
repack_hwm.csh
repack_st_arch-thru2013-12.csh
repack_st_arch.csh_proj2camp
repack_st_arch_tidy_mess.csh
sst+spinup.pptx
submit_compress_hist.csh
diags_batch.csh
not_in_DART
