library(tercen)
library(dplyr)

library(stringr)
library(jsonlite)

do.grid <- function(df, props, docId, imgInfo)
{
  sqcMinDiameter       <- 0.45 #as.numeric(props$sqcMinDiameter) #0.45
  segEdgeSensitivity   <- list(0, 0.01)
  qntSeriesMode        <- 0
  qntShowPamGridViewer <- 0
  grdSpotPitch         <- 21.5 #as.numeric(props$grdSpotPitch) #21.5
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
  
  jsonFile <- tempfile(fileext = ".json")
  on.exit(unlink(jsonFile))
  
  write(jsonData, jsonFile)
  
  system(paste("/mcr/exe/pamsoft_grid \"--param-file=", jsonFile[1], "\"", sep=""))
  
  
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

  outFrame <- data.frame( 
    .ci = rep(df$.ci[1], nGrid),
    grdIsReference = isRefChar,
    qntSpotID = griddingOutput$qntSpotID,
    grdRow = as.double(griddingOutput$grdRow),
    grdCol = as.double(griddingOutput$grdCol),
    grdXOffset = as.double(griddingOutput$grdXOffset),
    grdYOffset = as.double(griddingOutput$grdYOffset),
    grdXFixedPosition = as.double(griddingOutput$grdXFixedPosition),
    grdYFixedPosition = as.double(griddingOutput$grdYFixedPosition),
    gridX = as.double(griddingOutput$gridX),
    gridY = as.double(griddingOutput$gridY),
    grdRotation = as.double(griddingOutput$grdRotation),
    grdImageNameUsed = griddingOutput$grdImageNameUsed
  )
  on.exit(unlink(outputfile))
  
  return(outFrame)
}

get_operator_props <- function(ctx, imagesFolder){
  sqcMinDiameter <- -1
  grdSpotPitch   <- -1
  
  operatorProps <- ctx$query$operatorSettings$operatorRef$propertyValues
  
  for( prop in operatorProps ){
    if (prop$name == "sqcMinDiameter"){
      sqcMinDiameter <- prop$value
    }
    
    if (prop$name == "grdSpotPitch"){
      grdSpotPitch <- prop$value
    }
  }
  
  props <- list()
  
  props$sqcMinDiameter <- sqcMinDiameter
  props$grdSpotPitch <- grdSpotPitch
  
  
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
ctx = tercenCtx()

if (!any(ctx$cnames == "documentId")) stop("Column factor documentId is required") 
if (length(ctx$labels) == 0) stop("Label factor containing the image name must be defined") 

# Set LD_LIBRARY_PATH environment variable to speed calling pamsoft_grid multiple times
MCR_PATH <- "/opt/mcr/v99"
LIBPATH <- "."

MCR_PATH_1 <- paste(MCR_PATH, "runtime", "glnxa64", sep = "/")
MCR_PATH_2 <- paste(MCR_PATH, "bin", "glnxa64", sep = "/")
MCR_PATH_3 <- paste(MCR_PATH, "sys", "os", "glnxa64", sep = "/")
MCR_PATH_4 <- paste(MCR_PATH, "sys", "opengl", "lib", "glnxa64", sep = "/")

LIBPATH <- paste(LIBPATH,MCR_PATH_1, sep = ":")
LIBPATH <- paste(LIBPATH,MCR_PATH_2, sep = ":")
LIBPATH <- paste(LIBPATH,MCR_PATH_3, sep = ":")
LIBPATH <- paste(LIBPATH,MCR_PATH_4, sep = ":")

Sys.setenv( "LD_LIBRARY_PATH" = LIBPATH )
# ---------------------------------------
# END MCR Path setting


docId     <- unique( ctx %>% cselect(documentId)  )[1]
docId     <- docId$documentId

imgInfo   <- prep_image_folder(docId)
props     <- get_operator_props(ctx, imgInfo[1])

ctx$select( c('.ci', ctx$labels[[1]] )) %>% 
  group_by(.ci) %>% 
  do(do.grid(., props, docId, imgInfo)) %>%
  ctx$addNamespace() %>%
  ctx$save() 


