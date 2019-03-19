#  clusterSSH - launch R code in parallel on a cluster
#  Copyright (C) 2019  Georg Schnabel
#  
#  clusterSSH is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  
#  clusterSSH is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>


#!/bin/sh

# arguments to script

calcId="$1"
jobname="$2"
numJobs="$3"
jobscript="$4"

# internal variables

curStart=0
curEnd=0
remJobs=$numJobs
workPath="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# hack to remove error message
# /bin/sh: module: line 1: syntax error: unexpected end of file
# which occurs when qsub is wrapped within a bash script
unset module

touch "clusterStatus_${calcId}.running"

#############################################
#  awk script to get number of active jobs
#############################################

read -d '' getNumJobs << 'EOF'
BEGIN { activeJobs = 0 }
NR>=3 && $NF ~ /^[0-9]+-[0-9]+:[0-9]+$/ {
  split($NF,a,/[-:]/)
  queuedJobs = int((a[2] - a[1] + 1) / a[3])
  activeJobs = activeJobs + queuedJobs
}
NR>=3 && $NF ~ /^[0-9]+$/ {
  activeJobs = activeJobs + 1
}
END { print activeJobs }
EOF

##############################################
#  loop until all jobs done
##############################################

isRunning=1 # 0 if job runs, something else otherwise

while [ $remJobs -gt 0 -o $isRunning -eq 0 ]; do

  # determine number of available cores

  myRunJobs=`qstat | awk "$getNumJobs"`
  totRunJobs=`qstat -u '*' | awk "$getNumJobs"`

  totCoresAvail=$((470-totRunJobs))
  if [ $totCoresAvail -lt 0 ]; then
    totCoresAvail=0
  fi
  myCoresAvail=$((200-myRunJobs))
  if [ $myCoresAvail -lt 0 ]; then
    myCoresAvail=0
  fi

  if [ $myCoresAvail -lt $totCoresAvail ]; then
    coresAvail=$myCoresAvail
  else
    coresAvail=$totCoresAvail
  fi

  if [ $remJobs -lt $coresAvail ]; then
    coresAvail=$remJobs
  fi

  #echo total cores available: $totCoresAvail
  #echo cores available for me: $myCoresAvail
  #echo effective number of cores available: $coresAvail

#############################################
#  execute new jobs if cores available
#############################################

  # inserted to deal with jobs not starting and entering Eqw state
  if [ $coresAvail -gt 0 -o $remJobs -eq 0 ]; then

     # facon brutale de traiter les calculs erronnes
qstat -xml | tr '\n' ' ' | sed 's#<job_list[^>]*>#\n#g' | sed 's#<[^>]*>##g' | grep " " | column -t | awk '
{
  jobid=substr($3,5)
  if ($5=="r")
    split($9,taskary,",")
  else
    split($8,taskary,",")

  for (s in taskary) {
    jobtaskStr = jobid "." taskary[s]
    if ($5=="r")
      isRunning[jobtaskStr] = isRunning[jobtaskStr] + 1
    if ($5=="qw")
      isQueued[jobtaskStr] = isQueued[jobtaskStr] + 1
    if ($5=="Eqw")
      isEqw[jobtaskStr] = isEqw[jobtaskStr] + 1

    uniquejobid = $1 "." taskary[s]
    print uniquejobid
    jobtaskStrDic[uniquejobid] = jobtaskStr
    taskstate[uniquejobid] = $5
    submitCmd[uniquejobid] = "qsub -t " taskary[s] " clusterJobscript_" jobid ".csh"
    deleteCmd[uniquejobid] = "qdel " uniquejobid
    combinedCmd[uniquejobid] = submitCmd[uniquejobid] " && " deleteCmd[uniquejobid]
  }
}

END {
  for (s in jobtaskStrDic) {
    t = jobtaskStrDic[s]
    if (isRunning[t]) {
      if (taskstate[s] != "r")
        system(deleteCmd[s])
      else if (isRunning[t]>1) {
        system(deleteCmd[s])
        isRunning[t] = isRunning[t] - 1
      }
    } else if (isQueued[t]) {
      if (taskstate[s] != "qw")
        system(deleteCmd[s])
      else if (isQueued[t]>1) {
        system(deleteCmd[s])
        isQueued[t] = isQueued[t] - 1
      }
    } else if (taskstate[s] == "Eqw") {
        system(combinedCmd[s])
    }
  }
}'

  fi

  if [ $coresAvail -gt 0 ]; then

    curStart=$((curEnd+1))
    curEnd=$((curEnd+coresAvail))

    qsub -t ${curStart}-${curEnd} "$jobscript"

    remJobs=$((remJobs-coresAvail))
  fi

  sleep 5
  qstat -j $jobname
  isRunning=$?

done

rm "clusterStatus_${calcId}.running"

