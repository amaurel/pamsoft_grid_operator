# Architecture Comparison: Before and After

## Original Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Tercen Platform                           │
│                  (Data Storage & API)                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                R Operator (main.R)                           │
│  ┌────────────────────────────────────────────────────┐     │
│  │  1. Download images from Tercen                    │     │
│  │  2. Prepare operator properties                    │     │
│  │  3. Build execution queue                          │     │
│  │     - Map groups to cores                          │     │
│  │     - Schedule batches                             │     │
│  │  4. Generate N JSON files (one per group)          │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐     │
│  │  For each BATCH of groups (grouped by queue):     │     │
│  │  ┌──────────────────────────────────────────┐     │     │
│  │  │  Spawn M MATLAB processes in parallel     │     │     │
│  │  │  (M = number of available cores)          │     │     │
│  │  └──────────────────────────────────────────┘     │     │
│  │  Wait for all M processes to complete             │     │
│  │  Read M separate CSV output files                 │     │
│  │  Aggregate results                                 │     │
│  │  Update progress (batch completed)                │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  5. Combine all results                                     │
│  6. Upload to Tercen                                        │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   │ (processx::process$new for each group)
                   │
    ┌──────────────┴──────────────┬──────────────────┐
    ▼                             ▼                  ▼
┌────────────┐              ┌────────────┐      ┌────────────┐
│  MATLAB    │              │  MATLAB    │ ...  │  MATLAB    │
│  Process 1 │              │  Process 2 │      │  Process N │
│            │              │            │      │            │
│ Group 1    │              │ Group 2    │      │ Group N    │
│ Images     │              │ Images     │      │ Images     │
│   ↓        │              │   ↓        │      │   ↓        │
│ Gridding   │              │ Gridding   │      │ Gridding   │
│   ↓        │              │   ↓        │      │   ↓        │
│ Output CSV │              │ Output CSV │      │ Output CSV │
└────────────┘              └────────────┘      └────────────┘

Total Processes: 1 (R) + N (MATLAB) = N+1 processes
```

### Characteristics
- **R Complexity**: HIGH (queue management, process orchestration)
- **Process Count**: N+1 (one R, N MATLAB)
- **Memory**: N × MATLAB runtime overhead
- **Coordination**: R manages process lifecycle
- **Progress**: Updated per batch completion
- **Error Handling**: Must handle N potential failure points

---

## Refactored Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Tercen Platform                           │
│                  (Data Storage & API)                        │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│          R Operator (main_refactored.R)                      │
│  ┌────────────────────────────────────────────────────┐     │
│  │  1. Download images from Tercen                    │     │
│  │  2. Prepare operator properties                    │     │
│  │  3. Generate SINGLE batch JSON                     │     │
│  │     - All groups in one config                     │     │
│  │     - Worker count specified                       │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐     │
│  │  4. Launch single MATLAB batch process             │     │
│  │  5. Monitor progress file (/tmp/progress.txt)      │     │
│  │     - Update Tercen events in real-time            │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  6. Parse aggregated CSV results                            │
│  7. Upload to Tercen                                        │
└──────────────────┬───────────────────────────────────────────┘
                   │
                   │ (single processx::process$new)
                   │
                   ▼
┌──────────────────────────────────────────────────────────────┐
│           MATLAB Batch Process                               │
│           (pamsoft_grid_batch)                               │
│                                                              │
│  ┌────────────────────────────────────────────────────┐     │
│  │  Read batch configuration JSON                     │     │
│  │  Initialize parpool (M workers)                    │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐     │
│  │  parfor i = 1:N  (N groups, M workers)           │     │
│  │    ┌──────────────────────────────────────┐       │     │
│  │    │  Worker 1: Process Group 1            │       │     │
│  │    │  Worker 2: Process Group 2            │       │     │
│  │    │  ...                                  │       │     │
│  │    │  Worker M: Process Group M            │       │     │
│  │    │  (then Groups M+1, M+2, etc.)        │       │     │
│  │    └──────────────────────────────────────┘       │     │
│  │    Each worker:                                   │     │
│  │      - Load images                                │     │
│  │      - Preprocess                                 │     │
│  │      - Grid detection                             │     │
│  │      - Segmentation                               │     │
│  │      - Return results to main thread              │     │
│  │                                                    │     │
│  │    Update progress file after each group          │     │
│  └────────────────────────────────────────────────────┘     │
│                                                              │
│  ┌────────────────────────────────────────────────────┐     │
│  │  Aggregate results from all groups                 │     │
│  │  Write single CSV output                           │     │
│  └────────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────────┘
                   │
                   ▼
            Single CSV Output
          /tmp/batch_results.csv

Total Processes: 1 (R) + 1 (MATLAB with M workers) = 2 processes
```

### Characteristics
- **R Complexity**: LOW (thin wrapper)
- **Process Count**: 2 (one R, one MATLAB)
- **Memory**: Single MATLAB runtime + worker pool
- **Coordination**: MATLAB parpool manages workers
- **Progress**: Real-time updates during execution
- **Error Handling**: Single failure point with context

---

## Side-by-Side Code Comparison

### Job Configuration

#### Original
```r
# main.R:19-60 - One JSON per group
prep_grid_files <- function(df, props, docId, imgInfo, grp, tmpDir){
  baseFilename <- paste0(tmpDir, "/grd_", grp, "_")
  # ... extract images for this group ...

  dfJson = list(
    "sqcMinDiameter"=props$sqcMinDiameter,
    "grdSpotPitch"=props$grdSpotPitch,
    # ... all parameters ...
    "outputfile"=paste0(baseFilename, '_grid.txt'),
    "imageslist"=unlist(imageList)
  )

  jsonData <- toJSON(dfJson, pretty=TRUE, auto_unbox=TRUE)
  jsonFile <- paste0(baseFilename, '_param.json')
  write(jsonData, jsonFile)
}

# Called for each group
df %>%
  group_by(.ci) %>%
  group_walk(~ prep_grid_files(.x, props, docId, imgInfo, .y, tmpDir))
```

#### Refactored
```r
# aux_functions_refactored.R - Single JSON for all groups
prep_batch_config <- function(df, props, imgInfo, tmpDir, numCores){
  groups <- unique(df$.ci)
  imageGroups <- list()

  for(grp in groups){
    grpDf <- df %>% filter(.ci == grp)
    # ... extract images for this group ...

    groupConfig <- list(
      groupId = as.character(grp),
      sqcMinDiameter = props$sqcMinDiameter,
      # ... all parameters ...
      imageslist = unlist(imageList)
    )
    imageGroups[[length(imageGroups) + 1]] <- groupConfig
  }

  batchConfig <- list(
    mode = "batch",
    numWorkers = numCores,
    progressFile = paste0(tmpDir, "/progress.txt"),
    outputFile = paste0(tmpDir, "/batch_results.csv"),
    imageGroups = imageGroups
  )

  jsonFile <- paste0(tmpDir, "/batch_config.json")
  write(toJSON(batchConfig, auto_unbox=TRUE), jsonFile)
  return(jsonFile)
}

# Called once
jsonFile <- prep_batch_config(df, props, imgInfo, tmpDir, nCores)
```

### Execution

#### Original
```r
# main.R:63-107 - Spawn N processes
do.grid <- function(df, tmpDir){
  grpCluster <- unique(df$.ci)

  procList = lapply(grpCluster, function(grp) {
    jsonFile <- paste0(tmpDir, "/grd_", grp, "_param.json")
    MCR_PATH <- "/opt/mcr/v99"

    p <- processx::process$new(
      "/mcr/exe/run_pamsoft_grid.sh",
      c(MCR_PATH, paste0("--param-file=", jsonFile)),
      stdout = outLog
    )
    return(list(p = p, out = outLog))
  })

  # Wait for all processes
  for (pObj in procList) {
    pObj$p$wait(timeout = 1000 * 60 * 10)
    # ... error handling ...
  }

  # Parse results from each group
  for(grp in grpCluster) {
    outputfile <- paste0(tmpDir, "/grd_", grp, "_grid.txt")
    griddingOutput <- read.csv(outputfile)
    # ... aggregate ...
  }
}

# Execute in batches
df %>%
  group_by(queu) %>%
  do(do.grid(., tmpDir))
```

#### Refactored
```r
# aux_functions_refactored.R - Single process
run_matlab_batch <- function(jsonFile, ctx){
  MCR_PATH <- "/opt/mcr/v99"

  p <- processx::process$new(
    "/mcr/exe/run_pamsoft_grid_batch.sh",
    c(MCR_PATH, paste0("--param-file=", jsonFile)),
    stdout = outLog
  )

  # Monitor progress file
  progressFile <- paste0(dirname(jsonFile), "/progress.txt")
  while(p$is_alive()) {
    Sys.sleep(5)
    if(file.exists(progressFile)) {
      progress <- readLines(progressFile, n=1)
      # Update Tercen events
    }
  }

  p$wait(timeout = 1000 * 60 * 30)
  return(p$get_exit_status())
}

# Execute once
exitCode <- run_matlab_batch(jsonFile, ctx)
```

---

## Key Metrics

| Metric | Original | Refactored | Change |
|--------|----------|------------|--------|
| R Lines of Code | 268 | 120 | -55% |
| Process Count | N+1 | 2 | -98% (for N=100) |
| JSON Files | N | 1 | -99% |
| CSV Outputs | N | 1 | -99% |
| Complexity (R) | O(N) | O(1) | Constant |
| Progress Updates | Per batch | Per group | Real-time |
| Error Handling | N points | 1 point | Simplified |

---

## When to Use Which

### Use Original (main.R)
- ✅ Need to debug single groups
- ✅ Limited MATLAB licensing
- ✅ Testing/development phase
- ✅ Small number of groups (N < 4)

### Use Refactored (main_refactored.R)
- ✅ Production workloads
- ✅ Large number of groups (N > 10)
- ✅ Memory-constrained environments
- ✅ Need real-time progress
- ✅ MATLAB Parallel Computing Toolbox available
