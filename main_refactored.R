library(tercen)
library(tercenApi)
library(dplyr)
library(stringr)
library(jsonlite)
library(tiff)
library(processx)

library(tim)
library(tools)

source('aux_functions_refactored.R')

ctx = tercenCtx()

# =====================
# MAIN OPERATOR CODE - REFACTORED
# =====================
# MATLAB handles parallelization internally
# R is a thin wrapper for Tercen integration

colNames <- ctx$cnames

# Validate input columns
docIdCols <- unname(unlist(colNames[unlist(lapply(colNames, function(x){
  return(grepl("documentId", x, fixed = TRUE))
}))]))

if (length(docIdCols) == 0 || length(docIdCols) > 2) {
  stop("Either 1 or 2 documentId columns expected.")
}

if (length(ctx$labels) == 0) {
  stop("Label factor containing the image name must be defined")
}

# Download images from Tercen
imgInfo <- prep_image_folder(ctx, docIdCols)
props <- get_operator_props(ctx, imgInfo)

task <- ctx$task
tmpDir <- tempdir()

# Get input data
df <- ctx$select(c('.ci', ctx$labels[[1]]))

# Determine number of groups and request resources
groups <- unique(df$.ci)
nGroups <- length(groups)
nCpusRequested <- min(nGroups, 8) # Cap at 8 cores for MATLAB parpool

ctx$requestResources(
  nCpus = nCpusRequested,
  ram = 500000000,
  ram_per_cpu = 500000000
)

nCores <- ctx$availableCores()

# Send initial progress
if(!is.null(task)){
  evt <- TaskProgressEvent$new()
  evt$taskId <- task$id
  evt$total <- nGroups
  evt$actual <- 0
  evt$message <- paste0("Preparing batch processing for ", nGroups, " image groups")
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

# Create single batch configuration JSON
jsonFile <- prep_batch_config(df, props, imgInfo, tmpDir, nCores)

# Send progress update
if(!is.null(task)){
  evt <- TaskProgressEvent$new()
  evt$taskId <- task$id
  evt$total <- nGroups
  evt$actual <- 0
  evt$message <- "Starting MATLAB batch processing"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

# Execute MATLAB batch processor (single process, internal parallelization)
exitCode <- run_matlab_batch(jsonFile, ctx)

if(exitCode != 0) {
  stop(paste("MATLAB batch processing failed with exit code:", exitCode))
}

# Parse aggregated results
outputFile <- paste0(tmpDir, "/batch_results.csv")
results <- parse_batch_results(outputFile, df)

# Send final progress
if(!is.null(task)){
  evt <- TaskProgressEvent$new()
  evt$taskId <- task$id
  evt$total <- nGroups
  evt$actual <- nGroups
  evt$message <- "Uploading results to Tercen"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}

# Save results to Tercen
results %>%
  ctx$addNamespace() %>%
  ctx$save()

# Cleanup
unlink(jsonFile)
unlink(outputFile)
unlink(paste0(tmpDir, "/progress.txt"))
