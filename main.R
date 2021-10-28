library(tercen)
library(plyr)
library(dplyr)

library(stringr)
library(jsonlite)

library(processx)

do.grid <- function(df, tmpDir){
  
  grpCluster <- unique(df$.ci)
  
  
  actual = get("actual",  envir = .GlobalEnv) + 1
  total = get("total",  envir = .GlobalEnv) 
  
  assign("actual", actual, envir = .GlobalEnv)
  
  MCR_PATH <- "/opt/mcr/v99"
  procList <- list()
  for(grp in grpCluster)
  {
    
    baseFilename <- paste0( tmpDir, "/", grp, "_")
    jsonFile <- paste0(baseFilename, '_param.json')
    
    p<-processx::process$new("/mcr/exe/run_pamsoft_grid.sh", 
                             c(MCR_PATH, 
                               paste0("--param-file=", jsonFile[1])),
                             stdout = "|", stderr="|")
    
    procList <- append( procList, p )
  }
  
  
  # Wait for all processes to finish
  for(p in procList)
  {
    # Wait for 10 minutes then times out
    p$wait(timeout = 1000 * 60 * 10)
  }
  
  outDf <- NULL
  
  
  for(grp in grpCluster)
  {
    baseFilename <- paste0( tmpDir, "/", grp, "_")
    jsonFile <- paste0(baseFilename, '_param.json')
    
    # The rest of the code should be very similar
    outputfile <- paste0(baseFilename, "_grid.txt") 
    
    griddingOutput <- read.csv(outputfile, header = TRUE)
    nGrid          <- nrow(griddingOutput)
    
    griddingOutput$grdIsReference <- as.logical(griddingOutput$grdIsReference)
    
    outFrame <- data.frame( 
      .ci = rep(df$.ci[1], nGrid),
      IsReference = isRefChar,
      ID = griddingOutput$qntSpotID,
      spotRow = as.double(griddingOutput$grdRow),
      spotCol = as.double(griddingOutput$grdCol),
      grdXOffset = as.double(griddingOutput$grdXOffset),
      grdYOffset = as.double(griddingOutput$grdYOffset),
      grdXFixedPosition = as.double(griddingOutput$grdXFixedPosition),
      grdYFixedPosition = as.double(griddingOutput$grdYFixedPosition),
      gridX = as.double(griddingOutput$gridX),
      gridY = as.double(griddingOutput$gridY),
      grdRotation = as.double(griddingOutput$grdRotation),
      grdImageNameUsed = griddingOutput$grdImageNameUsed
    )
    
    if(is.null(outDf)){
      outDf <- outFrame
    }else{
      outDf <- rbind(outDf, outFrame)
    }
    
    
    # Cleanup
    unlink(jsonFile)
    unlink(outputfile)
  }
  
  if(!is.null(task)){
    evt = TaskProgressEvent$new()
    evt$taskId = task$id
    evt$total = total
    evt$actual = actual
    evt$message = paste0("Performing gridding (",  as.integer(100.0*(actual)/total),"%)")
    ctx$client$eventService$sendChannel(task$channelId, evt)
  }
  
  
  
  return(outDf)
}

get_operator_props <- function(ctx, imagesFolder){
  sqcMinDiameter <- -1
  grdSpotPitch   <- -1
  grdSpotSize   <- -1
  
  operatorProps <- ctx$query$operatorSettings$operatorRef$propertyValues
  
  for( prop in operatorProps ){
    if (prop$name == "MinDiameter"){
      sqcMinDiameter <- prop$value
    }
    
    if (prop$name == "SpotPitch"){
      grdSpotPitch <- prop$value
    }
    
    if (prop$name == "SpotSize"){
      grdSpotSize <- prop$value
    }
  }
  
  if( is.null(grdSpotPitch) || grdSpotPitch == -1 ){
    grdSpotPitch <- 21.5
  }
  
  if( is.null(grdSpotSize) || grdSpotSize == -1 ){
    grdSpotSize <- 0.66
  }
  
  if( is.null(sqcMinDiameter) || sqcMinDiameter == -1 ){
    sqcMinDiameter <- 0.45
  }
  
  props <- list()
  
  props$sqcMinDiameter <- sqcMinDiameter
  props$grdSpotPitch <- grdSpotPitch
  props$grdSpotSize <- grdSpotSize
  
  
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


prep_grid_files <- function(df, props, docId, imgInfo, grp, tmpDir){
  
  sqcMinDiameter       <- as.numeric(props$sqcMinDiameter) 
  segEdgeSensitivity   <- list(0, 0.01)
  qntSeriesMode        <- 0
  qntShowPamGridViewer <- 0
  grdSpotPitch         <- as.numeric(props$grdSpotPitch) 
  grdSpotSize          <- as.numeric(props$grdSpotSize) 
  grdUseImage          <- "Last"
  pgMode               <- "grid"
  dbgShowPresenter     <- "no"
  #-----------------------------------------------
  # END of property setting
  
  baseFilename <- paste0( tmpDir, "/", grp, "_")
  
  colNames  <- names(df)
  imageList <- pull( df, colNames[2]) 
  
  
  for(i in seq_along(imageList)) {
    imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/" )
    imageList[i] <- paste(imageList[i], imgInfo[2], sep = "." )
  }
  
  
  
  outputfile <- paste0(baseFilename, '_grid.txt') #tempfile(fileext=".txt") 
  
  
  
  dfJson = list("sqcMinDiameter"=sqcMinDiameter, 
                "segEdgeSensitivity"=segEdgeSensitivity,
                "qntSeriesMode"=qntSeriesMode,
                "qntShowPamGridViewer"=qntShowPamGridViewer,
                "grdSpotPitch"=grdSpotPitch,
                "grdUseImage"=grdUseImage,
                "pgMode"=pgMode,
                "dbgShowPresenter"=dbgShowPresenter,
                "arraylayoutfile"=props$arraylayoutfile,
                "outputfile"=outputfile, "imageslist"=unlist(imageList))
  
  
  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox = TRUE)
  
  jsonFile <- paste0(baseFilename, '_param.json') #tempfile(fileext = ".json")
  
  
  write(jsonData, jsonFile)
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

# =====================
# MAIN OPERATOR CODE
# =====================
ctx = tercenCtx()


if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 
if (length(ctx$labels) == 0) stop("Label factor containing the image name must be defined") 


docId     <- unique( ctx %>% cselect(documentId)  )[1]
docId     <- docId$documentId

imgInfo   <- prep_image_folder(docId)
props     <- get_operator_props(ctx, imgInfo[1])


task = ctx$task

if( !is.null(task)){
  task = ctx$task
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$message =  "Performing gridding ... Please wait"
  ctx$client$eventService$sendChannel(task$channelId, evt)
}


tmpDir <- tempdir()


df <- ctx$select( c('.ci', ctx$labels[[1]] )) 

# Prepare processor queu
groups <- unique(df$.ci)
nCores <- parallel::detectCores() 
queu <- list()

currentCore <- 1
order <- 1
k <- 1

while(k <= length(groups)){
  for(i in 1:nCores){
    queu <- append( queu, order )
    k <- k + 1
    if( k > length(groups)){break}
  }
  order <- order + 1
}


assign("actual", 0, envir = .GlobalEnv)
assign("total", max(unlist(queu)), envir = .GlobalEnv)

df$queu <- mapvalues(df$.ci, 
                     from=groups, 
                     to=unlist(queu) )

# Preparation step
df %>% 
  group_by(.ci)   %>%
  group_walk(~ prep_grid_files(.x, props, docId, imgInfo, .y, tmpDir) ) 


qtTable %>% 
  group_by(queu)   %>%
  do(do.grid(., tmpDir)  ) %>%
  ungroup() %>%
  select(-queu) %>%
  arrange(.ci) %>%
  ctx$addNamespace() %>%
  ctx$save() 



#ctx$select( c('.ci', ctx$labels[[1]] )) %>% 
#  group_by(.ci) %>% 
#  partition(cluster = cluster) %>%
#  do(do.grid(., props, docId, imgInfo)) %>%
#  collect() %>%
#  ctx$addNamespace() %>%
#  ctx$save() 


