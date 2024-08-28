# This file is for user convenience only and is not used by the model
# Changes to this file will be ignored and overwritten
# Changes to the environment should be made in env_mach_specific.xml
# Run ./case.setup --reset to regenerate this file
source $LMOD_ROOT/lmod/init/sh
module load cesmdev/1.0 ncarenv/23.09
module purge 
module load craype cmake intel/2023.2.1 mkl ncarcompilers/1.0.0 cmake cray-mpich/8.1.27 netcdf-mpi/4.9.2 parallel-netcdf/1.12.3 parallelio/2.6.2 esmf/8.5.0
export OMP_STACKSIZE=64M
export FI_CXI_RX_MATCH_MODE=hybrid
export FI_MR_CACHE_MONITOR=memhooks
export NETCDF_PATH=/glade/u/apps/derecho/23.09/spack/opt/spack/netcdf/4.9.2/cray-mpich/8.1.27/oneapi/2023.2.1/wplx
export MPICH_MPIIO_HINTS=*:romio_cb_read=enable:romio_cb_write=enable:striping_factor=24