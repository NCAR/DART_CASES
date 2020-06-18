#!/bin/tcsh

# Last mod time before this comment was 2019-9-7.
# ? Script to use with qcmd?

set y_m = 2018-10

./mv_to_campaign.csh  $y_m ${s}/${casename}/archive/rest/$y_m \
                  /gpfs/csfs1/cisl/dares/Reanalyses/${casename}/rest

./mv_to_campaign.csh  $y_m ${s}/${casename}/archive/esp/hist/$y_m \
                  /gpfs/csfs1/cisl/dares/Reanalyses/${casename}/esp/hist

./mv_to_campaign.csh  $y_m ${s}/${casename}/archive/atm/hist/$y_m \
                  /gpfs/csfs1/cisl/dares/Reanalyses/${casename}/atm/hist

./mv_to_campaign.csh  $y_m ${s}/${casename}/archive/logs/$y_m \
                  /gpfs/csfs1/cisl/dares/Reanalyses/${casename}/logs
