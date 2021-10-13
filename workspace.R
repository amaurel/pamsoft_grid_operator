library(tercen)
library(dplyr)

library(stringr)
library(jsonlite)

library(multidplyr)

do.grid <- function(df, props, docId, imgInfo, totalDoExec)
{
  sqcMinDiameter       <- as.numeric(props$sqcMinDiameter) #0.45
  segEdgeSensitivity   <- list(0, 0.01)
  qntSeriesMode        <- 0
  qntShowPamGridViewer <- 0
  grdSpotPitch         <- as.numeric(props$grdSpotPitch) #21.5
  grdSpotSize         <- as.numeric(props$grdSpotSize) #21.5
  grdUseImage          <- "Last"
  pgMode               <- "grid"
  dbgShowPresenter     <- "no"
  #-----------------------------------------------
  # END of property setting
  
  colNames  <- names(df)
  imageList <- pull( df, colNames[2]) 
  
  
  for(i in seq_along(imageList)) {
    imageList[i] <- paste(imgInfo[1], imageList[i], sep = "/" )
    imageList[i] <- paste(imageList[i], imgInfo[2], sep = "." )
  }
  
  
  
  outputfile <- tempfile(fileext=".txt") 
  on.exit(unlink(outputfile))
  
  
  dfJson = list("sqcMinDiameter"=sqcMinDiameter, 
                "segEdgeSensitivity"=segEdgeSensitivity,
                "qntSeriesMode"=qntSeriesMode,
                "qntShowPamGridViewer"=qntShowPamGridViewer,
                "grdSpotPitch"=grdSpotPitch,
                "grdSpotSize"=grdSpotSize,
                "grdUseImage"=grdUseImage,
                "pgMode"=pgMode,
                "dbgShowPresenter"=dbgShowPresenter,
                "arraylayoutfile"=props$arraylayoutfile,
                "outputfile"=outputfile, "imageslist"=unlist(imageList))
  
  
  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox = TRUE)
  
  jsonFile <- tempfile(fileext = ".json")
  on.exit(unlink(jsonFile))
  
  write(jsonData, jsonFile)
  
  MCRROOT="/home/rstudio/mcr/v99/"
  system(paste("/home/rstudio/pg_exe/run_pamsoft_grid.sh ", 
               MCRROOT,
               " \"--param-file=", jsonFile[1], "\"", sep=""))
  
  
  griddingOutput <- read.csv(outputfile, header = TRUE)
  nGrid          <- nrow(griddingOutput)
  
  isRefChar <-  c()
  for(i in seq_along(griddingOutput$grdIsReference)) {
    if (griddingOutput$grdIsReference[i] == 1){
      isRefChar[i] <-"TRUE"
    }else{
      isRefChar[i] <-"FALSE"
    }


  }

  #TODO Change column names as the array layout file
  outFrame <- data.frame( 
    .ci = rep(df$.ci[1], nGrid),
    grdIsReference = isRefChar,
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


  return(outFrame)
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


prep_image_folder <- function(docId){
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
  return(list(imageResultsPath, fext))
  
}

# =====================
# MAIN OPERATOR CODE
# =====================
#http://127.0.0.1:5402/admin/w/cff9a1469cd1de708b87bca99f003d42/ds/a1eae81d-db4c-4f71-baff-888d42351ab4
options("tercen.workflowId" = "cff9a1469cd1de708b87bca99f003d42")
options("tercen.stepId"     = "a1eae81d-db4c-4f71-baff-888d42351ab4")

ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 
if (length(ctx$labels) == 0) stop("Label factor containing the image name must be defined") 


docId     <- unique( ctx %>% cselect(documentId)  )[1]
docId     <- docId$documentId

imgInfo   <- prep_image_folder(docId)
props     <- get_operator_props(ctx, imgInfo[1])

totalDoExec <- nrow(unique( ctx$select( ".ci" )))


# SETTING up parallel processing
nCores <- parallel::detectCores()
cluster <- new_cluster(nCores-4)


cluster_copy(cluster, "do.grid")    

cluster_assign(cluster, props= props)    
cluster_assign(cluster, imgInfo=imgInfo)    
cluster_assign(cluster, docId=docId)
cluster_assign(cluster, totalDoExec=totalDoExec)

cluster_library(cluster, "tercen")
cluster_library(cluster, "dplyr")
cluster_library(cluster, "stringr")
cluster_library(cluster, "jsonlite")


ctx$select( c('.ci', ctx$labels[[1]] )) %>% 
  group_by(.ci) %>% 
  partition(cluster = cluster) %>%
  do(do.grid(., props, docId, imgInfo, toalDoExec)) %>%
  collect() %>%
  ctx$addNamespace() %>%
  ctx$save() 




# DEBUG ONLY
# Clean up temporary folder with images
dirs = list.dirs(path = "/tmp/", full.names = TRUE, recursive = FALSE)

for (dirpath in dirs ){
  if (grepl( "Rtmp", dirpath, fixed = TRUE)){
    system(paste("rm -rf ", dirpath, "/*", sep=""))
  }
}

# END OF DEBUG-ONLY CODE
