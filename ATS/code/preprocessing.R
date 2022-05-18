source("libraries.R")
source("model.R")
source("model-prediction.R")
source("utilities.R")

PATH_IN <- "../sublogs/original/"
PATH_OUT <- "../sublogs/preprocessed/"
PATH_SKIP <- "../sublogs/preprocessed_done/"
prjFiles <- list.files(PATH_IN)

prefix2exclude <- "individual_specialist_tfidf_k5_"


all <- read.csv("../sublogs/all_preprocessed.csv", stringsAsFactors=FALSE)

allnumbers <- NULL
for (i in 1:length(prjFiles)) {
   dest_filename <- paste(PATH_SKIP,gsub(prefix2exclude, "", prjFiles[i]),sep="")
   if (! file.exists(dest_filename) ) {
      g <- read.csv(file=paste(PATH_IN,prjFiles[i],sep=""), stringsAsFactors=TRUE)
      # pre-processing: no need, since we are using the pre-proc all file
      dist <- distinct(g, number)
      ds <- all[as.factor(all$number) %in% dist$number,]
      write.csv(ds, file=gsub(PATH_SKIP, PATH_OUT, dest_filename), row.names=FALSE)
      #write.csv(ds, file=paste(prjPathOut,gsub(prefix2exclude, "", prjFiles[i]),sep=""), row.names=FALSE)
      allnumbers <- rbind(allnumbers, dist)
   }
}
