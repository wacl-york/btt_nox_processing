print(Sys.getenv("FILE_SELECT"))

para = eddy4R.york::def.para(
  DirWrk = "/processing/ec",
  DirInp = "in/2023",
  siteName = "BTT",
  analysis = stringr::str_remove(basename(Sys.getenv("FILE_SELECT")), ".csv"),
  runID = "standard_2023",
  fileMask = "NOx_5Hz_%y_%m_%d_%H%M%S.csv",
  species = c("NO","NO2"),
  aggregationPeriod = 3600,
  fileDuration = 3600,
  lat = 51.5215, #CHECK
  AlgBase = "trnd",
  idepVar = "unixTime",
  MethRot = "double",
  missingMethod = "mean",
  lagApplyCorrection = TRUE,
  lagApplyRangeLimit = TRUE,
  lagNOc = TRUE,
  writeFastData = TRUE,
  lagRangeLimit = list(c(0,0),
                       c(0,0),
                       c(0,-10),
                       c(0,-10)
  ),
  lagDefaults = c(0,0,-6,-6))

# pass the (in-container) path as the FILE_SELECT environment variable
matching = para$filePaths %in% Sys.getenv("FILE_SELECT")
para$filePaths = para$filePaths[matching]
para$fileNames = para$fileNames[matching]

eddy4R.york::wrap.towr(para)