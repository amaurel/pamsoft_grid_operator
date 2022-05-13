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

prep_image_folder <- function(ctx, docIdCols){
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
  imagesFolder <- imgInfo[1]
  
  
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
  layoutDir <- paste(imgInfo[3], "*Layout*", sep = "/")
  props$arraylayoutfile <- Sys.glob(layoutDir)
  
  return (props)
}