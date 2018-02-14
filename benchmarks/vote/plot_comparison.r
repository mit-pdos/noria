library(dplyr)

args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (arg in args) {
	a <- strsplit(sub(".log", "", basename(arg)), "\\.")
	a <- a[[1]]
	server = a[[1]]
	articles = as.numeric(sub("a$", "", a[[2]]))
	target = as.numeric(sub("t$", "", a[[3]]))
	#read_perc = as.numeric(sub("r$", "", a[[4]]))

	con <- file(arg, open = "r")
	actual = 0
	in_results = TRUE
	ts <- data.frame()
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
		if (startsWith(line, "# ")) {
			if (startsWith(line, "# actual ops")) {
				actual = actual + as.numeric(sub("# actual ops/s: ", "", line))
				in_results = TRUE
			} else if (startsWith(line, "# server stats")) {
				in_results = FALSE
			}
		} else if (in_results) {
			v <- read.table(text = line)
			this <- data.frame(op=v[,1], pct=as.factor(v[,2]), sjrn=as.numeric(v[,3]), rmt=as.numeric(v[,4]))
			ts <- rbind(ts, this)
		}
	} 

	dt <- data.frame(age=rchisq(20,10),group=sample(1:2,20,rep=T))
	ts <- ts %>% group_by(op, pct) %>% summarize(sjrn = max(sjrn), rmt = max(rmt))

	ts$server = server
	ts$articles = articles
	ts$target = target
	ts$actual = actual
	#ts$readp = read_perc
	ts <- data.frame(ts)

	t <- rbind(t, ts)
	close(con)
}
#t
t = t[t$pct != 100,]
t$server <- as.factor(t$server)
t$actual <- t$actual/1000000.0
t$target <- t$target/1000000.0
mx_rmt = max(t[t$pct == 95,]$rmt)
t$sjrn <- pmin(t$sjrn, 10*mx_rmt) # otherwise ggplot tries to plot all the way to 100k

library(ggplot2)
pb <- ggplot(data=t, aes(x=target, y=rmt, color=server, linetype=op, shape=server))
pb <- pb + facet_wrap(~ pct)
pb <- pb + geom_point(size = 0.7, alpha = 0.8) + geom_line()
pb <- pb + coord_trans(x = "identity", y = "identity", limy=c(0, mx_rmt))
pb <- pb + xlab("target load (Mops/s)") + ylab("batch processing time (µs)") + ggtitle("Batch processing time")
ggsave('plot-batch.png',plot=pb,width=10,height=4)

ps <- ggplot(data=t, aes(x=target, y=sjrn, color=server, linetype=op, shape=server))
ps <- ps + coord_trans(x = "identity", y = "identity", limy=c(0, 1.2*mx_rmt))
ps <- ps + facet_wrap(~ pct)
ps <- ps + geom_point(size = 0.7, alpha = 0.8) + geom_line()
ps <- ps + xlab("target load (Mops/s)") + ylab("sojourn time (µs)") + ggtitle("Sojourn time")
ggsave('plot-sjrn.png',plot=ps,width=10,height=4)

library(plotly)
p <- subplot(pb, ps, nrows = 2, margin = 0.1)
htmlwidgets::saveWidget(p, "plots.html", selfcontained=F)
browseURL("plots.html")
