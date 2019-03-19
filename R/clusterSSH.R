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

#' Initialize a Cluster
#'
#' Initialize a cluster running a job scheduling engine such as the Sun Grid Engine (SGE) software
#'
#' @param userFuns a list which contain at least the functions \code{printId}, \code{start}, and \code{isRunning}
#' @param remFun object as returned from the \code{initSSH} function of the \code{remoteFunctionSSH} package
#' @param sshOpts named list of arguments passed to \code{remoteFunctionSSH}
#'
#' @return
#' Returns a list which contains the following functions:
#'
#' \code{start(fun, input, inputsPerTask=1, runOpts)}
#'
#' \code{eval(fun, input, inputsPerTask=1, runOpts, pollTime)}
#'
#' \code{result(job, selection=TRUE)}
#'
#' \code{numberOfTasks(job)}
#'
#' \code{closeCon()}
#'
#' \code{status(job, raw=FALSE)}
#'
#' The meaning of the arguments is
#' \tabular{ll}{
#'   \code{fun} \tab a function taking as argument a list \code{input};
#'   it must return a list of the same length as \code{input}
#'   where each element of the resulting list contains
#'   the result associated with the respective element in \code{input}\cr
#'   \code{input} \tab a list where each element is an input for a calculation\cr
#'   \code{inputsPerTask} \tab how many elements in \code{input} should be submitted as one task\cr
#'   \code{runOpts} \tab a list with arguments for the SGE engine, which are
#'   \code{cputime} (seconds), \code{vmem}, and \code{fsize} (megabytes). \cr
#'   \code{pollTime} \tab pause in seconds between two consecutive queries of the job status\cr
#'   \code{selection} \tab select for which tasks the results should returned\cr
#'   \code{raw} \tab should raw output of qstat be returned
#' }
#'
#' The return values of the functions are
#' \tabular{ll}{
#' \code{start} \tab returns \code{job} object\cr
#' \code{eval} \tab returns the results associated with the elements of \code{input}\cr
#' \code{result} \tab returns the same as \code{eval}\cr
#' \code{status} \tab returns either a dataframe with information about the tasks or
#'  a character vector with the raw output of \code{qstat}
#' }
#'
#' @import remoteFunctionSSH
#' @export
#'
initCluster <- function(userFuns, remFun=NULL, sshOpts=NULL) {

  stopifnot(!is.null(remFun) || !is.null(sshOpts),
            is.list(userFuns),
            c("start","isRunning","printId") %in% names(userFuns),
            is.function(userFuns$start) && is.function(userFuns$isRunning),
            ! any(c("eval","result","closeCon","remFun","setUserFuns","clearData",
                    "startJob","isRunningJob") %in% names(userFuns)))

  # init connection
  RscriptPath <- NULL # to be filled

  if (is.null(remFun)) {
    RscriptPath <- sshOpts$Rscript
    sshOpts$use.exist <- FALSE # avoid double uploading
    remFun <- do.call(initSSH,sshOpts)
  } else {
    RscriptPath <- remFun$Rscript
  }
  closeCon <- remFun$closeCon

  # user supplied functions for the remote side

  clusterObj <- list()
  startJob <- NULL # to be filled
  isRunningJob <- NULL # to be filled

  setUserFuns <- function(userFuns) {

    for (curFunName in names(userFuns)) {
      if (!is.function(userFuns[[curFunName]])) {
        closeCon()
        stop(paste0("The supplied object ",curFunName," in userFuns is not a function"))
      }
      if (curFunName %in% c("printId","start","isRunning")) next
      clusterObj[[curFunName]] <<- remFun$createRemoteFunction(
        userFuns[[curFunName]], fun.name=paste0(userFuns$printId(),"_",curFunName),
        cache=FALSE, transfer=FALSE, show.output=FALSE)
    }

    startJob <<- remFun$createRemoteFunction(
      userFuns$start, fun.name=paste0(userFuns$printId(),"_startJob"),
      cache=FALSE, transfer=FALSE, show.output=FALSE)

    isRunningJob <<- remFun$createRemoteFunction(
      userFuns$isRunning, fun.name=paste0(userFuns$printId(),"_isRunningJob"),
      cache=FALSE, transfer=FALSE, show.output=FALSE)

    remFun$updateAndTransfer()

  }

  # main functions

  prepareJob <- function(fun, input, runOpts=list(), inputsPerTask=1) {

    library(digest)
    calcId <- digest(list(fun,input), algo="sha1")
    jobname <- paste0("JOB_",calcId)

    splitFact <- rep(1:length(input),each=inputsPerTask)
    groups <- tapply(seq_along(splitFact),splitFact,identity)
    numTasks <- length(groups)

    funDataDir <- remFun$getFunDataDir(paste0(userFuns$printId(),"_startJob"))
    workspaceFile <- sprintf("clusterScript_%s.R",calcId)
    workspacePath <- file.path(funDataDir,workspaceFile)
    inputFiles <- character(length(groups))
    resultFiles <- character(length(groups))

    remFunDataDir <- remFun$getRemFunDataDir(paste0(userFuns$printId(),"_startJob"))
    remGlobalDir <- remFun$getRemGlobalDir()
    remPackageDir <- remFun$getRemPackageDir()
    remWorkspacePath <- file.path(remFunDataDir,workspaceFile)

    # prepare the input files and also specify result filenames
    for (curGroupIdx in seq_along(groups)) {
      subIdx <- groups[[curGroupIdx]]
      curInput <- input[subIdx]
      curInputFile <- sprintf("clusterInput_%s_%03d.RData",calcId,curGroupIdx)
      curResultFile <- sprintf("clusterOutput_%s_%03d.RData",calcId,curGroupIdx)
      saveRDS(curInput,file.path(funDataDir,curInputFile))
      inputFiles[curGroupIdx] <- curInputFile
      resultFiles[curGroupIdx] <- curResultFile
    }

    remInputPaths <- file.path(remFunDataDir,inputFiles)
    remResultPaths <- file.path(remFunDataDir,resultFiles)
    remSharedDataDir <- getRemSharedDataDir()

    # save the function wrapper
    dput(substitute({
      sapply(list.files(globalDir,pattern="*.rda",full.names = TRUE),load,.GlobalEnv)
      sapply(list.files(globalDir,pattern="*.R",full.names = TRUE),source,.GlobalEnv)
      .libPaths(c(.libPaths(),packageDir))
      local({
        funx <- fun
        environment(funx) <- .GlobalEnv
        oldwd <- getwd()
        dir.create(sharedDataDir, showWarnings = FALSE)
        setwd(sharedDataDir)
        saveRDS(funx(readRDS(inputFiles[as.integer(commandArgs(trailingOnly=TRUE)[1])])),
                file=resultFiles[as.integer(commandArgs(trailingOnly=TRUE)[1])])
        setwd(oldwd)
      })
    },env=list(fun=fun, globalDir=remGlobalDir, packageDir=remPackageDir,
               inputFiles=remInputPaths, resultFiles=remResultPaths,
               sharedDataDir=remSharedDataDir)),
    file=workspacePath)

    # prepare job object
    jobObj <- list(calcId=calcId,
                   jobname=jobname,
                   numTasks=numTasks,
                   # working directories
                   locDir=funDataDir,
                   remDir=remFunDataDir,
                   # files associated with the job
                   workspaceFile=workspaceFile,
                   inputFiles=inputFiles,
                   resultFiles=resultFiles
    )

    # transfer files and start job
    remFun$uploadFunData(paste0(userFuns$printId(),"_startJob"),c(workspaceFile,inputFiles))
    Rcmd <- paste0("'",RscriptPath,"' --vanilla '",remWorkspacePath,"' ")
    jobObj <- startJob(jobObj, runOpts, Rcmd)
    jobObj
  }

  numberOfTasksJob <- function(jobObj) {
    return(jobObj$numTasks)
  }

  resultJob <- function(jobObj, selection=TRUE) {

    remFun$downloadFunData(paste0(userFuns$printId(),"_startJob"),jobObj$resultFiles)
    basedir <- remFun$getFunDataDir(paste0(userFuns$printId(),"_startJob"))
    resultFiles <- file.path(basedir, jobObj$resultFiles)
    resultFiles <- resultFiles[selection]
    unlist(lapply(resultFiles,function(file) {
      try(readRDS(file),silent=TRUE)
      }),recursive = FALSE)
  }

  evalJob <- function(fun, input, runOpts=list(), inputsPerTask=1, pollTime = c(10,120)) {
    stopifnot(is.numeric(pollTime), length(pollTime)<=2, length(pollTime)>0)
    if (length(pollTime)==1) pollTime <- rep(pollTime, 2)

    jobObj <- prepareJob(fun, input, runOpts, inputsPerTask)
    while (isRunningJob(jobObj)) { Sys.sleep(runif(1,pollTime[1],pollTime[2])) }
    resultJob(jobObj)
  }

  clearData <- function(del.rem=TRUE, del.loc=TRUE) {
    remFun$clearFunData(paste0(userFuns$printId(),"_startJob"),del.rem=del.rem, del.loc=del.loc)
  }

  #################################
  # handling of additional data
  #################################

  getSharedDataDir <- function() {
    sharedDir <- clusterObj$remFun$getFunDataDir(paste0(userFuns$printId(),"_startJob"))
    sharedDir <- file.path(sharedDir,"sharedData")
    dir.create(sharedDir,showWarnings=FALSE)
    sharedDir
  }

  getRemSharedDataDir <- function() {
    remSharedDir <- clusterObj$remFun$getRemFunDataDir(paste0(userFuns$printId(),"_startJob"))
    remSharedDir <- file.path(remSharedDir,"sharedData")
    remSharedDir
  }

  uploadSharedData <- function(files="") {
    files <- file.path("sharedData",files)
    clusterObj$remFun$uploadFunData(paste0(userFuns$printId(),"_startJob"),files)
  }

  downloadSharedData <- function(files="", ignore.missing=FALSE) {
    files <- file.path("sharedData",files)
    funName <- paste0(userFuns$printId(),"_startJob")
    isAvail <- rep(TRUE, length(files))
    if (isTRUE(ignore.missing))
      isAvail <- clusterObj$remFun$existsRemFunData(funName, files)

    clusterObj$remFun$downloadFunData(funName, files[isAvail])
  }

  clearSharedData <- function(files="",del.rem=TRUE,del.loc=TRUE) {
    files <- file.path("sharedData",files)
    clusterObj$remFun$clearFunData(paste0(userFuns$printId(),"_startJob"),files,del.rem,del.loc)
  }


  setUserFuns(userFuns)

  # job control
  clusterObj$start <- prepareJob
  clusterObj$isRunning <- isRunningJob
  clusterObj$numberOfTasks <- numberOfTasksJob
  clusterObj$result <- resultJob
  clusterObj$eval <- evalJob
  # data control
  clusterObj$getSharedDataDir <- getSharedDataDir
  clusterObj$getRemSharedDataDir <- getRemSharedDataDir
  clusterObj$uploadSharedData <- uploadSharedData
  clusterObj$downloadSharedData <- downloadSharedData
  clusterObj$clearSharedData <- clearSharedData
  # auxiliary
  clusterObj$clearData <- clearData
  clusterObj$closeCon <- closeCon
  clusterObj$remFun <- remFun
  clusterObj$setUserFuns <- setUserFuns

  clusterObj
}

