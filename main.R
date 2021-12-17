library(tercen)
library(plyr)
library(dplyr)

library(stringr)
library(jsonlite)


library(processx)



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
  grdSpotPitch       <- 21.5
  grdSpotSize        <- 0.66
  grdRotation        <- 0
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
  baseFilename <- paste0( tmpDir, "/grd_", grp, "_")
  
  
  colNames  <- names(df)
  
  imageCol <- which(rapply(as.list(colNames), str_detect, pattern=".Image"))
  imageList <- pull( df, colNames[imageCol]) 
  
  
  for(i in seq_along(imageList)) {
    imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/" )
    imageList[i] <- paste(imageList[i], imgInfo[2], sep = "." )
  }
  
  
  outputfile <- paste0(baseFilename, '_grid.txt') 
  

  dfJson = list("sqcMinDiameter"=props$sqcMinDiameter,
                "sqcMaxDiameter"=props$sqcMaxDiameter,
                "segEdgeSensitivity"=props$segEdgeSensitivity,
                "qntSeriesMode"=0,
                "qntShowPamGridViewer"=0,
                "grdSpotPitch"=props$grdSpotPitch,
                "grdSpotSize"=props$grdSpotSize,
                "grdRotation"=props$grdRotation,
                "qntSaturationLimit"=props$qntSaturationLimit,
                "segMethod"=props$segMethod,
                "grdUseImage"="Last",
                "pgMode"="grid",
                "dbgShowPresenter"=0,
                "arraylayoutfile"=props$arraylayoutfile,
                "outputfile"=outputfile, "imageslist"=unlist(imageList))
  
  
  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox = TRUE)
  
  jsonFile <- paste0(baseFilename, '_param.json') 
  
  
  write(jsonData, jsonFile)
}


do.grid <- function(df, tmpDir){
  ctx = tercenCtx()
  task = ctx$task
  
  grpCluster <- unique(df$.ci)
  
  
  actual = get("actual",  envir = .GlobalEnv) + 1
  total = get("total",  envir = .GlobalEnv) 
  assign("actual", actual, envir = .GlobalEnv)
  
  
  procList = lapply(grpCluster, function(grp) {
    baseFilename <- paste0( tmpDir, "/grd_", grp, "_")

    jsonFile <- paste0(baseFilename, '_param.json')
    MCR_PATH <- "/opt/mcr/v99"

    outLog <- tempfile(fileext = '.log')

    p <- processx::process$new("/mcr/exe/run_pamsoft_grid.sh",
                               c(MCR_PATH,
                                 paste0("--param-file=", jsonFile[1])),
                               stdout = outLog)

    return(list(p = p, out = outLog))
  })
  
  # Wait for all processes to finish
  for (pObj in procList)
  {
    # Wait for 10 minutes then times out
    pObj$p$wait(timeout = 1000 * 60 * 10)
    exitCode <- pObj$p$get_exit_status()
    
    if (exitCode != 0) {
      for (pObj2 in procList) {
        if (pObj2$p$is_alive()) {
          print(paste0('kill process -- ' ))
          print(pObj$p)
          pObj2$p$kill()
        }
      }
      stop(readChar(pObj$out, file.info(pObj$out)$size))
    }
  }

  outDf <- NULL
  
  colNames  <- names(df)
  imageCol <- colNames[which(rapply(as.list(colNames), str_detect, pattern=".Image"))]
  
  for(grp in grpCluster)
  {
    baseFilename <- paste0( tmpDir, "/grd_", grp, "_")
    jsonFile <- paste0(baseFilename, '_param.json')
    outputfile <- paste0(baseFilename, "_grid.txt") 
    
    griddingOutput <- read.csv(outputfile, header = TRUE)
    nGrid          <- nrow(griddingOutput)
    
    isRefChar<- as.character(as.logical(griddingOutput$grdIsReference))

    
    gridCi <- df %>%
      filter(get(imageCol) == griddingOutput$grdImageNameUsed[1]) %>% 
      pull(.ci)

    outFrame <- data.frame( 
      .ci = as.integer(rep(gridCi, nGrid)),
      IsReference = isRefChar,
      ID = griddingOutput$qntSpotID,
      spotRow = as.double(griddingOutput$grdRow),
      spotCol = as.double(griddingOutput$grdCol),
      grdXFixedPosition = as.double(griddingOutput$grdXFixedPosition),
      grdYFixedPosition = as.double(griddingOutput$grdYFixedPosition),
      gridX = as.double(griddingOutput$gridX),
      gridY = as.double(griddingOutput$gridY),
      diameter = as.double(griddingOutput$diameter),
      manual = as.double(as.logical(griddingOutput$isManual)),
      bad = as.double(as.logical(griddingOutput$segIsBad)),
      empty = as.double(as.logical(griddingOutput$segIsEmpty)),
      grdRotation = as.double(griddingOutput$grdRotation),
      grdImageNameUsed = griddingOutput$grdImageNameUsed
    )
    

    
    if(is.null(outDf)){
      outDf <- outFrame
    }else{
      outDf <- rbind(outDf, outFrame)
    }
    
    
    # Cleanup files
    unlink(jsonFile)
    unlink(outputfile)
  }
  
  if(!is.null(task)){
    evt = TaskProgressEvent$new()
    evt$taskId = task$id
    evt$total = total
    evt$actual = actual
    evt$message = paste0("Performing gridding: ",  actual, "/", total)
    ctx$client$eventService$sendChannel(task$channelId, evt)
  }
  
  return(outDf)
}



# =====================
# MAIN OPERATOR CODE
# =====================
# http://localhost:5402/admin/w/11143520a88672e0a07f89bb88075d15/ds/4b30b4a9-d299-4d4b-8cd3-27f34c4bcb64
# options("tercen.workflowId" = "11143520a88672e0a07f89bb88075d15")
# options("tercen.stepId"     = "4b30b4a9-d299-4d4b-8cd3-27f34c4bcb64")

ctx = tercenCtx()


if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 
if (length(ctx$labels) == 0) stop("Label factor containing the image name must be defined") 


docId     <- unique( ctx %>% cselect(documentId)  )[1]
docId     <- docId$documentId

imgInfo   <- prep_image_folder(docId)
props     <- get_operator_props(ctx, imgInfo[1])


task = ctx$task

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

if( !is.null(task)){
  evt = TaskProgressEvent$new()
  evt$taskId = task$id
  evt$total = max(unlist(queu))
  evt$actual = 0
  evt$message = paste0("Performing gridding: ",  actual, "/", total)
  ctx$client$eventService$sendChannel(task$channelId, evt)
}


df$queu <- mapvalues(df$.ci, 
                     from=groups, 
                     to=unlist(queu) )

# Preparation step
df %>%
  group_by(.ci)   %>%
  group_walk(~ prep_grid_files(.x, props, docId, imgInfo, .y, tmpDir) )


df %>%
  group_by(queu)   %>%
  do(do.grid(., tmpDir)  ) %>%
  ungroup() %>%
  select(-queu) %>%
  arrange(.ci) %>%
  ctx$addNamespace() %>%
  ctx$save()



