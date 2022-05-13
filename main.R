library(tercen)
library(tercenApi)
library(plyr)
library(dplyr)
library(stringr)
library(jsonlite)
library(tiff)
library(processx)
library(parallelly)

library(tim)
library(tools)

source('aux_functions.R')


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

# Dual input
# http://127.0.0.1:5402/test-team/w/8ef9012b2d2f050214e16189ba0406b4/ds/4f23e372-640e-4779-92ce-fe180c447390
# options("tercen.workflowId" = "8ef9012b2d2f050214e16189ba0406b4")
# options("tercen.stepId"     = "4f23e372-640e-4779-92ce-fe180c447390")

# Single input
# http://127.0.0.1:5402/test-team/w/8ef9012b2d2f050214e16189ba0406b4/ds/e55ae97a-2fa8-4071-87d2-1997bcbfa7c0
#options("tercen.workflowId" = "8ef9012b2d2f050214e16189ba0406b4")
#options("tercen.stepId"     = "e55ae97a-2fa8-4071-87d2-1997bcbfa7c0")

ctx = tercenCtx()

colNames <- ctx$cnames

# Checking for documentId columns
docIdCols <- unname(unlist(colNames[unlist(lapply(colNames, function(x){
  return(grepl("documentId", x, fixed = TRUE))
} ))]))

if (length(docIdCols) == 0 || length(docIdCols) > 2) stop("Either 1 or 2 documentId columns expected.") 
if (length(ctx$labels) == 0) stop("Label factor containing the image name must be defined") 


#docId     <- unique( ctx %>% cselect(documentId)  )[1]
#docId     <- docId$documentId

#imgInfo   <- prep_image_folder(docId)
imgInfo   <- prep_image_folder(ctx, docIdCols)
props     <- get_operator_props(ctx, imgInfo)

task = ctx$task

tmpDir <- tempdir()

df <- ctx$select( c('.ci', ctx$labels[[1]] )) 

# Prepare processor queu
groups <- unique(df$.ci)
nCores <- parallelly::availableCores(methods="cgroups.cpuset")
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

# Execution step
df %>%
  group_by(queu)   %>%
  do(do.grid(., tmpDir)  ) %>%
  ungroup() %>%
  select(-queu) %>%
  arrange(.ci) %>%
  ctx$addNamespace() %>%
  ctx$save()




