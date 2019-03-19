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


# Functionality for Sun Grid Engine (SGE)

#' @export
functions_SGE <- list(

  printId = function() {
    "SGE"
  },

  start = function(jobObj, runOpts, Rcmd) {

    basedir <- getwd()

    jobObj$remScriptPath <- file.path(jobObj$remDir,sprintf("clusterScript_%s.sh",jobObj$calcId))

    # prepare other args
    otherSgeHeaderArgs <- paste0("#$ ",runOpts$otherArgs)
    sgeHeader <- c(
      "#!/bin/bash",
      "#$ -P P_mnm",
      paste0("#$ -N ",jobObj$jobname),
      paste0("#$ -t 1-",jobObj$numTasks),
      if (!is.null(runOpts$cputime))
        paste0("#$ -l ct=",sprintf("%d",runOpts$cputime)) else character(0),
      if (!is.null(runOpts$vmem))
        paste0("#$ -l vmem=",sprintf("%d",runOpts$vmem),"M") else character(0),
      if (!is.null(runOpts$fsize))
        paste0("#$ -l fsize=",sprintf("%d",runOpts$fsize),"M") else character(0),
      "#$ -m n -cwd -V",
      otherSgeHeaderArgs
    )
    fileCont <- c(sgeHeader,paste0(Rcmd," $SGE_TASK_ID"))
    write(fileCont,jobObj$remScriptPath)

    ret <- system(paste0("qsub '",jobObj$remScriptPath,"'"),intern=TRUE)
    if (isTRUE(attr(ret,"status")!=0)) {
      stop("an error occurred while submitting the job with qsub")
    }
    jobid <- regmatches(ret,regexec("job-array +([[:digit:]]+)",ret))[[1]][2]
    stopifnot(!is.na(jobid))
    jobObj$jobid <- jobid
    jobObj
  },


  isRunning = function(jobObj) {
    ret <- system(paste0("qstat -j '",jobObj$jobname,"'"))
    if (ret == 0) TRUE else FALSE
  },

  # convenience functions

  status = function(jobObj, raw=FALSE) {

    rawRes <- system(paste0("qstat -j ",jobObj$jobname),intern=TRUE)

    if (isTRUE(attr(rawRes,"status")!=0)) {
      if (isTRUE(raw))
        return(character(0))
      else
        return(data.frame(taskId=integer(0), state=character(0), startTime=character(0)))
    }
    if (isTRUE(raw))
      return(rawRes)

    # get total number of jobs
    tmp <- regmatches(rawRes,regexec("^ *job-array tasks: *1-([[:digit:]]+):",rawRes))
    idx <- sapply(tmp,length) > 0
    baseDf <- data.frame()
    if (any(idx)) {
      numTasks <- as.numeric(tmp[idx][[1]][2])
      baseDf <- data.frame(taskId=seq_len(numTasks))
    }
    # get jobstate
    tmp <- regmatches(rawRes,regexec("^ *job_state +([[:digit:]]+) *: *([[:alpha:]]) *$",rawRes))
    idx <- sapply(tmp,length) > 0
    jobState <- data.frame(taskId=as.integer(sapply(tmp[idx],function(x) x[2])),
                           state=sapply(tmp[idx],function(x) x[3]))
    # get start time
    tmp <- regmatches(rawRes,regexec("^ *start_time +([[:digit:]]+) *: *([[:digit:]].*)$",rawRes))
    idx <- sapply(tmp,length) > 0
    startTime <- data.frame(taskId=as.integer(sapply(tmp[idx],function(x) x[2])),
                            startTime=sapply(tmp[idx],function(x) x[3]))
    # merge the results
    res <- merge(jobState, startTime, all = TRUE)
    if (nrow(res) > 0) {
      res <- merge(baseDf, res, all = TRUE)
    } else if (nrow(baseDf)>0) {
      res <- cbind.data.frame(baseDf,state=NA_character_, startTime=NA_character_)
    } else {
      res <- data.frame(taskId=integer(0), state=character(0), startTime=character(0))
    }
    res
  },

  output = function(job,task=NULL) {
    basedir <- job$remDir
    pat <- paste0(job$jobname,"\\.o",job$jobid,"\\.([[:digit:]]+)")
    outfiles <- list.files(basedir,pat)
    regexRes <- regmatches(outfiles,regexec(pat,outfiles))
    taskIds <- as.integer(sapply(regexRes,function(x) x[2]))
    if (is.null(task)) task <- seq_along(outfiles)
    outfiles <- file.path(basedir,outfiles[order(taskIds)][task])
    errfiles <- sub(paste0("\\.o",job$jobid),paste0("\\.e",job$jobid),outfiles)
    lapply(seq_along(outfiles), function(x) list(out=readLines(outfiles[x]),
                                                 err=readLines(errfiles[x])))
  }

)

