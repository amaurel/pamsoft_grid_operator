# ------------------------------------------------------------------------
# Refactored auxiliary functions for MATLAB-driven parallelization
# Simplified version - MATLAB handles parallel execution
# ------------------------------------------------------------------------

prep_image_folder <- function(ctx, docIdCols){
  # Identical to original - downloads images from Tercen
  task = ctx$task
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = 1
  evt$actual = 0
  evt$message = "Downloading image files"
  ctx$client$eventService$sendChannel(task$channelId, evt)

  if(length(docIdCols) == 1){
    docIds <- ctx$cselect(docIdCols)

    f.names <- tim::load_data(ctx, unique(unlist(docIds[1])) )
    f.names <- grep('*/ImageResults/*', f.names, value = TRUE )

    imageResultsPath <- dirname(f.names[1])
    layoutDir <- dirname(imageResultsPath)
    fext <- file_ext(f.names[1])
    res <- (list(imageResultsPath, fext, layoutDir))
  }else{
    docIds <- ctx$cselect(docIdCols)

    f.names.a <- tim::load_data(ctx, unique(unlist(docIds[1])) )
    f.names.b <- tim::load_data(ctx, unique(unlist(docIds[2])))

    f.names <- grep('*/ImageResults/*', f.names.a, value = TRUE )
    a.names <- f.names.b

    if(length(f.names) == 0 ){
      f.names <- grep('*/ImageResults/*', f.names.b, value = TRUE )
      a.names <- f.names.a
    }

    if(length(f.names) == 0 ){
      stop("No ImageResults/ path found within provided files.")
    }

    imageResultsPath <- dirname(f.names[1])

    fext <- file_ext(f.names[1])
    layoutDir <- dirname(a.names[1])

    res <- list(imageResultsPath, fext, layoutDir)
  }

  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = 1
  evt$actual = 1
  evt$message = "Downloading image files"
  ctx$client$eventService$sendChannel(task$channelId, evt)
  return(res)
}


get_operator_props <- function(ctx, imgInfo){
  # Identical to original - extracts operator properties from Tercen context
  imagesFolder <- imgInfo[1]

  sqcMinDiameter     <- 0.45
  sqcMaxDiameter     <- 0.85
  grdSpotPitch       <- 0
  grdSpotSize        <- 0.66
  grdRotation        <- seq(-2, 2, by=0.25)
  qntSaturationLimit <- 4095
  segMethod          <- "Edge"
  segEdgeSensitivity <- list(0, 0.05)

  operatorProps <- ctx$query$operatorSettings$operatorRef$propertyValues

  for( prop in operatorProps ){

    if (prop$name == "Min Diameter"){
      sqcMinDiameter <- as.numeric(prop$value)
    }

    if (prop$name == "Max Diameter"){
      sqcMaxDiameter <- as.numeric(prop$value)
    }

    if (prop$name == "Rotation"){
      if(prop$value == "0"){
        grdRotation <- as.numeric(prop$value)
      }else{
        prts <- as.numeric(unlist(str_split(prop$value, ":")))
        grdRotation <- seq(prts[1], prts[3], by=prts[2])
      }
    }

    if (prop$name == "Saturation Limit"){
      qntSaturationLimit <- as.numeric(prop$value)
    }

    if (prop$name == "Spot Pitch"){
      grdSpotPitch <- as.numeric(prop$value)
    }

    if (prop$name == "Spot Size"){
      grdSpotSize <- as.numeric(prop$value)
    }

    if (prop$name == "Edge Sensitivity"){
      segEdgeSensitivity[2] <- as.numeric(prop$value)
    }
  }

  if( grdSpotPitch == 0 ){
    img_type <- get_imageset_type(imagesFolder)
    switch (img_type,
            "evolve3" = {grdSpotPitch<-17.0},
            "evolve2" = {grdSpotPitch<-21.5},
            "none"={stop("Cannot automatically detect Spot Pitch. Please set it to a value different than 0.")}
    )
  }

  props <- list()

  props$sqcMinDiameter <- sqcMinDiameter
  props$sqcMaxDiameter <- sqcMaxDiameter
  props$grdSpotPitch <- grdSpotPitch
  props$grdSpotSize <- grdSpotSize
  props$grdRotation <- grdRotation
  props$qntSaturationLimit <- qntSaturationLimit
  props$segEdgeSensitivity <- segEdgeSensitivity
  props$segMethod <- segMethod

  # Get array layout
  layoutDir <- paste(imgInfo[3], "*Layout*", sep = "/")
  props$arraylayoutfile <- Sys.glob(layoutDir)

  return (props)
}


get_imageset_type <- function(imgPath){
  # Identical to original - detects Evolve2 vs Evolve3
  exampleImage <- Sys.glob(paste(imgPath, "*tif", sep = "/"))[[1]]

  tiffHdr <- readTIFF(exampleImage, payload = FALSE)

  imgTypeTag <- "none"

  if( tiffHdr['width'] == 552 && tiffHdr['length'] == 413){
    imgTypeTag <- "evolve3"
  }

  if( tiffHdr['width'] == 697 && tiffHdr['length'] == 520){
    imgTypeTag <- "evolve2"
  }

  return(imgTypeTag)
}


prep_batch_config <- function(df, props, imgInfo, tmpDir, numCores){
  # NEW FUNCTION: Create batch configuration JSON for MATLAB
  # Groups all image processing jobs into single config

  colNames <- names(df)
  imageCol <- which(rapply(as.list(colNames), str_detect, pattern=".Image"))

  groups <- unique(df$.ci)
  imageGroups <- list()

  for(grp in groups){
    grpDf <- df %>% filter(.ci == grp)

    # Get image list for this group
    imageList <- pull(grpDf, colNames[imageCol])
    for(i in seq_along(imageList)) {
      imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/" )
      imageList[i] <- paste(imageList[i], imgInfo[2], sep = "." )
    }

    # Create group configuration
    groupConfig <- list(
      groupId = as.character(grp),
      sqcMinDiameter = props$sqcMinDiameter,
      sqcMaxDiameter = props$sqcMaxDiameter,
      segEdgeSensitivity = props$segEdgeSensitivity,
      qntSeriesMode = 0,
      qntShowPamGridViewer = 0,
      grdSpotPitch = props$grdSpotPitch,
      grdSpotSize = props$grdSpotSize,
      grdRotation = props$grdRotation,
      qntSaturationLimit = props$qntSaturationLimit,
      segMethod = props$segMethod,
      grdUseImage = "Last",
      pgMode = "grid",
      dbgShowPresenter = 0,
      arraylayoutfile = props$arraylayoutfile,
      imageslist = unlist(imageList)
    )

    imageGroups[[length(imageGroups) + 1]] <- groupConfig
  }

  # Create batch configuration
  batchConfig <- list(
    mode = "batch",
    numWorkers = numCores,
    progressFile = paste0(tmpDir, "/progress.txt"),
    outputFile = paste0(tmpDir, "/batch_results.csv"),
    imageGroups = imageGroups
  )

  # Write JSON
  jsonData <- toJSON(batchConfig, pretty = TRUE, auto_unbox = TRUE)
  jsonFile <- paste0(tmpDir, "/batch_config.json")
  write(jsonData, jsonFile)

  return(jsonFile)
}


run_matlab_batch <- function(jsonFile, ctx){
  # NEW FUNCTION: Execute MATLAB batch processor
  # Single process handles all groups

  task <- ctx$task
  MCR_PATH <- "/opt/mcr/v99"

  # Start MATLAB batch process
  outLog <- tempfile(fileext = '.log')

  p <- processx::process$new(
    "/mcr/exe/run_pamsoft_grid_batch.sh",
    c(MCR_PATH, paste0("--param-file=", jsonFile)),
    stdout = outLog,
    stderr = outLog
  )

  # Monitor progress file
  progressFile <- paste0(dirname(jsonFile), "/progress.txt")

  lastProgress <- "0/0: Initializing"
  checkInterval <- 5 # seconds

  while(p$is_alive()) {
    Sys.sleep(checkInterval)

    # Read progress file if exists
    if(file.exists(progressFile)) {
      progress <- readLines(progressFile, n = 1, warn = FALSE)

      if(length(progress) > 0 && progress != lastProgress) {
        lastProgress <- progress

        # Parse progress: "actual/total: message"
        parts <- str_split(progress, ": ", n = 2)[[1]]
        counts <- str_split(parts[1], "/")[[1]]

        if(!is.null(task)) {
          evt <- TaskProgressEvent$new()
          evt$taskId <- task$id
          evt$actual <- as.integer(counts[1])
          evt$total <- as.integer(counts[2])
          evt$message <- parts[2]
          ctx$client$eventService$sendChannel(task$channelId, evt)
        }
      }
    }
  }

  # Wait for completion (with 30 minute timeout)
  p$wait(timeout = 1000 * 60 * 30)
  exitCode <- p$get_exit_status()

  if (exitCode != 0) {
    logContent <- readChar(outLog, file.info(outLog)$size)
    stop(paste("MATLAB batch processing failed with exit code", exitCode,
               "\nLog:", logContent))
  }

  return(exitCode)
}


parse_batch_results <- function(outputFile, df){
  # NEW FUNCTION: Parse aggregated results from MATLAB
  # Returns data frame compatible with Tercen output

  if(!file.exists(outputFile)) {
    stop(paste("Output file not found:", outputFile))
  }

  # Read CSV results
  results <- read.csv(outputFile, header = TRUE, stringsAsFactors = FALSE)

  # Convert groupId back to .ci (integer)
  results$.ci <- as.integer(results$groupId)

  # Remove groupId column (not needed for Tercen output)
  results$groupId <- NULL

  # Convert character flags back to logical
  results$IsReference <- as.character(as.logical(results$grdIsReference))
  results$grdIsReference <- NULL

  # Ensure correct data types
  results$ID <- results$qntSpotID
  results$qntSpotID <- NULL

  results$spotRow <- as.double(results$grdRow)
  results$grdRow <- NULL

  results$spotCol <- as.double(results$grdCol)
  results$grdCol <- NULL

  results$grdXFixedPosition <- as.double(results$grdXFixedPosition)
  results$grdYFixedPosition <- as.double(results$grdYFixedPosition)
  results$gridX <- as.double(results$gridX)
  results$gridY <- as.double(results$gridY)
  results$diameter <- as.double(results$diameter)
  results$manual <- as.double(results$isManual)
  results$isManual <- NULL

  results$bad <- as.double(results$segIsBad)
  results$segIsBad <- NULL

  results$empty <- as.double(results$segIsEmpty)
  results$segIsEmpty <- NULL

  results$grdRotation <- as.double(results$grdRotation)

  # Arrange by .ci
  results <- results %>% arrange(.ci)

  return(results)
}
