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


# Functionality for Sun Grid Engine (SGE) without sheduler

#' @export
functions_simpleSGE <- function() {

  # get the bash script
  jobguardScript <- scan(file.path(path.package("clusterSSH"),"exec/simpleSGE_jobguard.sh"),
                          what=character(),sep="\n",quiet=TRUE,blank.lines.skip=FALSE)

  printId = function() {
    "simpleSGE"
  }

  start <- eval(parse(text=deparse(substitute(function(jobObj, runOpts, Rcmd) {

    jobguardScript <- insJobguardScript # trick: eval(substitute(quote(...))) to directly include the
                                        # character vector with the jobscript in the function code

    jobObj$remJobguardPath <- file.path(jobObj$remDir,sprintf("clusterJobguard_%s.sh",jobObj$calcId))
    jobObj$remJobscriptPath <- file.path(jobObj$remDir,sprintf("clusterJobscript_%s.csh",jobObj$calcId))

    # prepare other args
    otherSgeHeaderArgs <- paste0("#$ ",paste0(runOpts$otherArgs,collapse=" "))
    sgeHeader <- c(
      "#$ -S /bin/csh",
      "#$ -V -cwd -b n",
      paste0("#$ -N ",jobObj$jobname),
      otherSgeHeaderArgs
    )
    jobscript <- c(sgeHeader,paste0(Rcmd," $SGE_TASK_ID"))

    write(jobguardScript,file=jobObj$remJobguardPath)
    write(jobscript,jobObj$remJobscriptPath)

    cmdstr <- paste0("chmod 744 '",jobObj$remJobguardPath,"'")
    system(cmdstr) # set execute permissions

    cmdstr <- paste0("nohup ",
                     "'",jobObj$remJobguardPath,"' ",
                     "'",jobObj$calcId,"' ",
                     "'",jobObj$jobname,"' ",
                     "'",jobObj$numTasks,"' ",
                     "'",jobObj$remJobscriptPath,"' &")
    ret <- system(cmdstr)
    stopifnot(ret==0)

    jobObj$remRunningPath <- file.path(jobObj$remDir,sprintf("clusterStatus_%s.running",jobObj$calcId))

    timer <- Sys.time()
    while (!file.exists(jobObj$remRunningPath) && difftime(Sys.time(),timer,units="secs") < 5) { Sys.sleep(0.1) }

    jobObj
  }, list(insJobguardScript=jobguardScript)))))


  isRunning <- function(jobObj) {
    file.exists(jobObj$remRunningPath)
  }

  # convenience functions

  status <- function(jobObj, raw=FALSE) {

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
  }

  output <- function(job,task=NULL) {
    basedir <- job$remDir
    pat <- paste0(job$jobname,"\\.o[[:alnum:]]+\\.([[:digit:]]+)")
    outfiles <- list.files(basedir,pat)
    regexRes <- regmatches(outfiles,regexec(pat,outfiles))
    taskIds <- as.integer(sapply(regexRes,function(x) x[2]))
    if (is.null(task)) task <- seq_along(outfiles)
    outfiles <- file.path(basedir,outfiles[order(taskIds)][task])
    errfiles <- sub("\\.o([[:alnum:]]+)","\\.e\\1",outfiles)
    lapply(seq_along(outfiles), function(x) list(out=readLines(outfiles[x]),
                                                 err=readLines(errfiles[x])))
  }

  list(printId = printId,
       start = start,
       isRunning = isRunning,
       status = status,
       output=output
       )
}
