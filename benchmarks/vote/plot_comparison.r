args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (arg in args) {
	a <- strsplit(sub(".log", "", basename(arg)), "\\.")
	a <- a[[1]]
	server = a[[1]]
	articles = as.numeric(sub("a$", "", a[[2]]))
	target = as.numeric(sub("t$", "", a[[3]]))

	con <- file(arg, open = "r")
	actual = 0
	in_results = TRUE
	ts <- data.frame()
	this_t <- data.frame()
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
		if (startsWith(line, "# ")) {
			if (startsWith(line, "# actual ops")) {
				if (nrow(this_t) > 0) {
					if (nrow(ts) > 0) {
						ts <- ts + this_t
					} else {
						ts <- this_t
					}
					this_t <- data.frame()
				}
				actual = actual + as.numeric(sub("# actual ops/s: ", "", line))
				in_results = TRUE
			} else if (startsWith(line, "# server stats")) {
				in_results = FALSE
			}
		} else if (in_results) {
			v <- read.table(text = line)
			this <- data.frame(server=server, op=v[,1], pct=as.factor(v[,2]), sjrn=as.numeric(v[,3]), rmt=as.numeric(v[,4]), articles=articles, target=target, actual=actual)
			this_t <- rbind(this_t, this)
		}
	} 

	if (nrow(this_t) > 0) {
		if (nrow(ts) > 0) {
			ts <- ts + this_t
		} else {
			ts <- this_t
		}
	}

	t <- rbind(t, ts)
	close(con)
}
t
t = t[t$pct != 100,]
t$server <- as.factor(t$server)
t$actual <- t$actual/1000000.0
t$target <- t$target/1000000.0
#t$sjrn <- pmin(t$sjrn, 500) # otherwise ggplot tries to plot all the way to 100k

library(ggplot2)

p <- ggplot(data=t, aes(x=actual, y=rmt, color=server, linetype=op, shape=server))
#p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, 150))
p <- p + facet_wrap(~ pct)
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("offered load (Mops/s)") + ylab("batch processing time (µs)")
ggsave('plot-batch.png',plot=p,width=10,height=4)

p <- ggplot(data=t, aes(x=actual, y=sjrn, color=server, linetype=op, shape=server))
#p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, 150))
p <- p + facet_wrap(~ pct)
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("offered load (Mops/s)") + ylab("sojourn time (µs)")
ggsave('plot-sjrn.png',plot=p,width=10,height=4)
