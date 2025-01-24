library(ggplot2)
cat("\014") 
options(max.print=1000000)

t1 <- Sys.time()

H_FILE <- "FOR_PAPER_INSECURE_ANSWER.csv"
L_FILE <- "FOR_PAPER_NEUTRAL_ANSWER.csv"
h_dat <- read.csv(H_FILE)
h_val <- h_dat$REPO
l_dat   <- read.csv(L_FILE)
l_val   <- l_dat$REPO
y_limit <- c(0, 2500)
y_label <- 'NORM_USER_REPU'

h_val <- as.numeric(h_val)
l_val <- as.numeric(l_val)


doBoxPlot<- function(xLabelP, yLabelP, data2plot, limitP)
{
  box_plot <- ggplot(data2plot, aes(x=group, y=value, fill=group)) + geom_boxplot(width = 0.25, outlier.shape=16, outlier.size=1) + labs(x=xLabelP, y=yLabelP)
  box_plot<-  box_plot + theme(plot.title = element_text(hjust = 0.5), text = element_text(size=12), legend.position="none", axis.text=element_text(size=12))
  box_plot <- box_plot + scale_y_continuous(limits=limitP) 
  box_plot <- box_plot + stat_summary(fun.y=mean, geom="point", colour="darkred", shape=18, size=3) 
  box_plot
  
  pdf('RQ2_PLOT.pdf', width = 5, height = 2)
  print(box_plot)
  dev.off()
  
  
}

a <- data.frame(group = "Insecure Ans.", value = h_val)
b <- data.frame(group = "Neutral Ans.",  value = l_val)
# Combine into one long data frame
dataset_plot <- rbind(a, b)
doBoxPlot('UserReputation', y_label, dataset_plot, y_limit)

t2 <- Sys.time()
print(t2 - t1)  
rm(list = setdiff(ls(), lsf.str()))