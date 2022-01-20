# ------------------------------------------------------------------------
# Set of functions used to prepare images and read operator properties
# ------------------------------------------------------------------------

get_imageset_type <- function(imgPath){
  exampleImage <- Sys.glob(paste(imgPath, "*tif", sep = "/"))[[1]]
  
  tiffHdr <- readTIFF(exampleImage, payload = FALSE)
  
  imgTypeTag <- "none"
  
  if( tiffHdr['width'] == 552 && tiffHdr['length'] == 413){
    # Evolve3 Image Set
    imgTypeTag <- "evolve3"
  }
  
  if( tiffHdr['width'] == 697 && tiffHdr['length'] == 520){
    # Evolve2 Image Set
    imgTypeTag <- "evolve2"
  }
  
  return(imgTypeTag)
}

prep_image_folder <- function(docId){
  task = ctx$task
  
  
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = 1
  evt$actual = 0
  evt$message = "Downloading image files"
  ctx$client$eventService$sendChannel(task$channelId, evt)
  
  #1. extract files
  doc   <- ctx$client$fileService$get(docId )
  filename <- tempfile()
  writeBin(ctx$client$fileService$download(docId), filename)
  
  on.exit(unlink(filename, recursive = TRUE, force = TRUE))
  
  image_list <- vector(mode="list", length=length(grep(".zip", doc$name)) )
  
  # unzip archive (which presumably exists at this point)
  tmpdir <- tempfile()
  unzip(filename, exdir = tmpdir)
  
  imageResultsPath <- file.path(list.files(tmpdir, full.names = TRUE), "ImageResults")
  
  f.names <- list.files(imageResultsPath, full.names = TRUE)
  
  fdir <- str_split_fixed(f.names[1], "/", Inf)
  fdir <- fdir[length(fdir)]
  
  fname <- str_split(fdir, '[.]', Inf)
  fext <- fname[[1]][2]
  
  # Images for all series will be here
  
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = 1
  evt$actual = 1
  evt$message = "Downloading image files"
  ctx$client$eventService$sendChannel(task$channelId, evt)
  return(list(imageResultsPath, fext))
  
}

get_operator_props <- function(ctx, imagesFolder){
  sqcMinDiameter     <- 0.45
  sqcMaxDiameter     <- 0.85
  grdSpotPitch       <- 0
  grdSpotSize        <- 0.66
  grdRotation        <- seq(-2, 2, by=0.25)
  qntSaturationLimit <- 4095
  segMethod          <- "Edge"
  segEdgeSensitivity <- list(0, 0.05)
  
  
  # Quantification operator only
  isDiagnostic       <- TRUE
  
  
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
    
    if (prop$name == "Diagnostic Output"){
      if( prop$value == "Yes" ){
        isDiagnostic <- TRUE
      }else{
        isDiagnostic <- FALSE
      }
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
  props$isDiagnostic <- isDiagnostic
  
  
  # Get array layout
  layoutDirParts <- str_split_fixed(imagesFolder, "/", Inf)
  nParts  <- length(layoutDirParts) -1 # Layout is in parent folder
  
  layoutDir = ''
  
  for( i in 1:nParts){
    layoutDir <- paste(layoutDir, layoutDirParts[i], sep = "/")
  }
  layoutDir <- paste(layoutDir, "*Layout*", sep = "/")
  
  props$arraylayoutfile <- Sys.glob(layoutDir)
  
  return (props)
}