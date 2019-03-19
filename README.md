### clusterSSH - R package

This package enables the execution of R code on a cluster in parallel.
A function amenable to parallelization using this package must take 
one list as argument and return a list of the same length as the input list.
The i-th element in the returned list must represent the output of the 
obtained by processing the i-th element of the input list.

## Requirements

Communication with the cluster is done via the OpenSSH client available
under Mac and Linux. If SSH authentification via a password is desired,
the command line utility `sshpass` needs also to be installed.
R specific prerequisites are discussed in the next section.

## Installation

This package depends on the packages 
[interactiveSSH](https://github.com/gschnabel/interactiveSSH),
[rsyncFacility](https://github.com/gschnabel/rsyncFacility), and
[remoteFunctionSSH](https://github.com/gschnabel/remoteFunctionSSH)

You can use the following instructions in a terminal to install
all the packages at once. Packages already installed can be skipped.
```
git clone https://github.com/gschnabel/interactiveSSH.git
R CMD INSTALL interactiveSSH

git clone https://github.com/gschnabel/rsyncFacility.git
R CMD INSTALL rsyncFacility

git clone https://github.com/gschnabel/remoteFunctionSSH.git
R CMD INSTALL remoteFunctionSSH

git clone https://github.com/gschnabel/clusterSSH.git
R CMD INSTALL clusterSSH
```

## Basic usage

Different setups of computer clusters are possible. For this example, we assume
that:

* the worker nodes on the cluster have access to a shared filesystem,
* each worker node is accessible and the user can launch programs on them,
* one of the nodes, the login node, is accessible via SSH from the local
  computer and has also access to the shared filesystem. 

Several ways to coordinate the nodes are in principle possible but in this
example `ClusterSSH` will use the SSH connection to the login node to create
files on the shared filesystem to communicate with the worker nodes.
Controller scripts on the workers nodes detect newly created files
and take actions accordingly, such as starting a calculation.

In order to make communication work, a working directory, say `tempdir.loc`,
has to be chosen on the local machine and another one, say `tempdir.rem` has to
be selected on the remote machine (the worker node accessible via SSH).
These directories should be created before proceeding here.

The following R code executed locally establishes the connection to the
login node:

```{r}
library(remoteFunctionSSH)
library(clusterSSH)

remFunHnd <- initSSH("username@server", "password", 
                     tempdir.loc="tempdir.loc", tempdir.rem="tempdir.rem")
clusterHnd <- initCluster(functions_multinode, remFunHnd)
```
The first instruction after loading the libraries sets up the functionality to
transfer and execute R functions on the login node from the local R session.
It creates an object `remFunHnd` which enables access to that functionality.
The instruction afterwards creates R functions on the login node to handle 
calculations on the cluster and returns a list `clusterHnd` containing 
functions to prepare calculations and retrieve their results.
The first argument `functions_multinode` provides the specific functions needed
to manage a cluster with the features described above.

In the outlined cluster setup, the user needs to  manually launch a controller
script on each of the worker nodes. This script can be copied to the login node
in the following way:

```{r}
startNodeController <- remFunHnd$createRemoteFunction(
    functions_multinode$startNodeController, fun.name="startNodeController")
cmdstr <- startNodeController(clusterHnd)
cat(cmdstr,"\n")
```
This code chunk prints the command that needs to be executed on the worker
nodes in order to install the controllers.

Now everything is set up to apply a function to an input list in parallel.
Let's create a simple function that takes the numbers in the input list
and adds one to them:
```{r}
parFun <- function(input) {
    lapply(input, function(x) x+1)
}
```

This function can be applied to some input list:
```{r}
input <- list(1,10,20)
clusterHnd$eval(parFun, input, pollTime=5) 
```

