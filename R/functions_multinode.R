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


# Functionality for a cluster with one or more nodes
# a shared network drive and no job scheduler

#' @export
functions_multinode <- list(

  printId = function() {
    "multinode"
  },

  start = function(jobObj, runOpts, Rcmd) {

    stopifnot(is.list(runOpts) || is.null(runOpts))
    basedir <- getwd()
    jobControlDir <- file.path(jobObj$remDir, "jobControl")
    jobObj$jobControlDir <- jobControlDir

    dir.create(jobControlDir, showWarnings = FALSE)
    # prepare a directory containing the script to do the calculation
    curCalcDir <- file.path(jobControlDir, paste0("prepare_", jobObj$calcId))
    newCalcDir <- file.path(jobControlDir, paste0("running_", jobObj$calcId))

    dir.create(curCalcDir)
    for (curTaskId in seq_len(jobObj$numTasks)) {
      curTaskFile <- paste0("waiting_", sprintf("%04d", curTaskId))
      runTaskFile <- paste0("running_", sprintf("%04d", curTaskId))
      finTaskFile <- paste0("finished_", sprintf("%04d", curTaskId))
      outputFile <- paste0("output_", sprintf("%04d", curTaskId))
      curTaskPath <- file.path(curCalcDir, curTaskFile)

      oldTaskPath <- file.path(newCalcDir, curTaskFile)
      runTaskPath <- file.path(newCalcDir, runTaskFile)
      finTaskPath <- file.path(newCalcDir, finTaskFile)
      outputPath <- file.path(newCalcDir, outputFile)
      fileCont <- c(paste0("#!/bin/sh"),
                    paste0("# hostname: fillin"),
                    paste0("# calcdir: fillin"),
                    paste0(if (is.null(runOpts$TMPDIR)) "# " else "", "export TMPDIR='", runOpts$TMPDIR, "'"),
                    #paste0("mv '", oldTaskPath,"' '", runTaskPath,"'"), # another code takes responsibility for this
                    paste0(Rcmd, " ", curTaskId, " > '", outputPath,"'"),
                    paste0("mv '", runTaskPath, "' '", finTaskPath, "'"))
      write(fileCont, curTaskPath)
    }

    file.rename(curCalcDir, newCalcDir)
    jobObj
  },


  isRunning = function(jobObj) {

    jobControlDir <- file.path(jobObj$remDir, "jobControl")
    curCalcDir <- file.path(jobControlDir, paste0("running_", jobObj$calcId))
    if (dir.exists(curCalcDir)) {
      notFinishedFiles <- list.files(curCalcDir, "(^waiting_|^running_)", full.names=FALSE)
      if (length(notFinishedFiles)>0) TRUE else FALSE
    } else FALSE
  },

  # convenience functions

  status = function(jobObj, raw=FALSE) {

    runningJobDir <- file.path(jobObj$jobControlDir, paste0("running_", jobObj$calcId))
    finishedJobDir <- file.path(jobObj$jobControlDir, paste0("finished_", jobObj$calcId))

    df <- data.frame(calcId=rep(jobObj$calcId, jobObj$numTasks),
                     taskId=seq_len(jobObj$numTasks),
                     status=rep("waiting", jobObj$numTasks),
                     hostname=rep(NA_character_, jobObj$numTasks),
                     stringsAsFactors = FALSE)

    for (curJobDir in c(runningJobDir, finishedJobDir)) {
      if (!dir.exists(curJobDir)) next
      outputFiles <- list.files(curJobDir, "^running_", full.names=FALSE)
      taskNumbers <- as.numeric(sub("^running_", "", outputFiles))
      df$status[taskNumbers] <- "running"
      outputFiles <- list.files(curJobDir, "^finished_", full.names=FALSE)
      taskNumbers <- as.numeric(sub("^finished_", "", outputFiles))
      df$status[taskNumbers] <- "finished"
      # get additional info
      runningOrFinished <- df$status %in% c("running","finished")
      taskNumbers <- which(runningOrFinished)
      for (curTaskNumber in taskNumbers) {
        curFilename <- paste0(df$status[curTaskNumber], "_", sprintf("%04d", curTaskNumber))
        curFilePath <- file.path(curJobDir, curFilename)
        curFileContent <- scan(curFilePath, character(), sep="\n", quiet=TRUE)
        infoLineNum <- grep("# hostname:", curFileContent)
        curHostname <- sub("^# hostname: *([^ ]+) *$", "\\1", curFileContent[infoLineNum])
        df$hostname[curTaskNumber] <- curHostname
      }

    }

    df
  },

  output = function(jobObj,task=NULL) {

    basedir <- jobObj$remDir

    runningJobDir <- file.path(jobObj$jobControlDir, paste0("running_", jobObj$calcId))
    finishedJobDir <- file.path(jobObj$jobControlDir, paste0("finished_", jobObj$calcId))

    outputIndex <- rep(NA_character_, jobObj$numTasks)
    for (curJobDir in c(runningJobDir, finishedJobDir)) {
      if (!dir.exists(curJobDir)) next
      outputFiles <- list.files(curJobDir, "^output_", full.names=FALSE)
      taskNumbers <- as.numeric(sub("^output_", "", outputFiles))
      outputIndex[taskNumbers] <- file.path(curJobDir, outputFiles)
    }

    outputContent <- replicate(jobObj$numTasks, NULL, simplify = FALSE)
    if (is.null(task)) task <- seq_len(jobObj$numTasks)
    for (i in task) {
      if (!is.na(outputIndex[i]))
        outputContent[[i]] <- scan(outputIndex[i], what=character(), sep="\n", quiet=TRUE)
    }
    outputContent[task]
  },


  startNodeController = function(clustObj, maxNumCpus=Inf) {

    controlDir <- file.path(dirname(clustObj$getRemSharedDataDir()),"jobControl")
    dir.create(controlDir, showWarnings = FALSE)
    dput(substitute({

      args <- commandArgs(trailingOnly=TRUE)
      if (length(args)==0) {
        maxNumCpus <- Inf
      } else {
        maxNumCpus <- as.numeric(args[1])
      }

      controlDir <- insControlDir
      getNumTotCpus <- function() {
        cmdstr <- "awk '/processor[ \\t]*:[ \\t]*[0-9]+ */ { num++ }END{print num}' /proc/cpuinfo"
        as.numeric(system(cmdstr, intern=TRUE))
      }
      getNodeLoad <- function() {
        cmdstr <- "top -b -n 1 | awk '/%Cpu/ {print $2}'"
        as.numeric(sub(",",".",system(cmdstr, intern=TRUE))) / 100
      }

      numTotCpus <- getNumTotCpus()
      getNumAvailCpus <- function() {
        floor((1-getNodeLoad()) * numTotCpus)
      }

      getNumRunningTasks <- function() {
        cmdstr <- paste0("ps aux | grep -F 'sh ", file.path(controlDir, "running_"), "' | wc -l")
        as.numeric(system(cmdstr, intern=TRUE)) - 2
      }

      curHostname <- system("hostname", intern=TRUE)
      activeFile <- file.path(controlDir, paste0("active_", curHostname))
      if (file.exists(activeFile))
        stop(paste0("already running! If not, delete ", activeFile))


      write(Sys.time(), file=activeFile)
      while (file.exists(activeFile)) {

        Sys.sleep(5)
        runningJobDirs <- list.files(controlDir, "^running_", full.names=FALSE)
        if (length(runningJobDirs)==0) next

        runningJobPaths <- file.path(controlDir, runningJobDirs)
        numAvailCpus <- max(0, min(max(getNumAvailCpus(),1),
                                   min(numTotCpus, maxNumCpus) - getNumRunningTasks()))
        for (jobIdx in seq_along(runningJobDirs)) {
          curJobPath <- runningJobPaths[jobIdx]
          waitingTasks <- list.files(curJobPath, "^waiting_", full.names = FALSE)
          waitingTaskPaths <- file.path(curJobPath, waitingTasks)

          for (taskIdx in seq_along(waitingTasks)) {
            if (numAvailCpus<=0) break
            # moving the file ensures that only one controller executes the script (in case several run in parallel)
            newRunTaskPath <- file.path(curJobPath, sub("^waiting_", "running_", waitingTasks[taskIdx]))
            ret <- system(paste0("mv '", waitingTaskPaths[taskIdx], "' '",newRunTaskPath,"'"))
            if (ret != 0) next
            # add some info about the execution
            curHostname <- system("hostname",intern=TRUE)
            runFileContent <- scan(newRunTaskPath,character(),sep="\n",quiet=TRUE)
            infoLineNum <- grep("# hostname: fillin", runFileContent)
            runFileContent[infoLineNum] <- sub("fillin", curHostname, runFileContent[infoLineNum])
            write(runFileContent, file=newRunTaskPath)
            # run the job
            # system(paste0("nice sh ", newRunTaskPath), wait=FALSE)
            system(paste0("sh ", newRunTaskPath), wait=FALSE)
            numAvailCpus <- numAvailCpus - 1
          }
          if (length(waitingTasks)==0) {
            runningTasks <- list.files(curJobPath, "^running_", full.names = FALSE)
            if (length(runningTasks)==0) {
              newJobDir <- sub("^running_", "finished_", runningJobDirs[jobIdx])
              newJobPath <- file.path(controlDir, newJobDir)
              file.rename(curJobPath, newJobPath)
            }
          }
        }
      }
    }, env=list(insControlDir=controlDir)),file=file.path(controlDir, "nodeController.R"))

    cmdstr <- paste0("Rscript --vanilla ", file.path(controlDir, "nodeController.R"), " ",
		     as.character(maxNumCpus))
    # system(paste0("Rscript --vanilla ", file.path(controlDir, "nodeController.R")),wait=FALSE)
    cmdstr
  }

)

