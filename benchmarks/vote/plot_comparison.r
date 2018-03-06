library(dplyr)

args <- commandArgs(trailingOnly = TRUE)
t <- data.frame()
for (arg in args) {
	a <- strsplit(sub(".log", "", basename(arg)), "\\.")
	a <- a[[1]]
	server = a[[1]]
	articles = as.numeric(sub("a$", "", a[[2]]))
	target = as.numeric(sub("t$", "", a[[3]]))
	read_perc = as.numeric(sub("r$", "", a[[4]]))
	skew = a[[5]]

	con <- file(arg, open = "r")
	cpu = 0
	actual = 0
	clients = 0
	in_results = TRUE
	ts <- data.frame()
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
		if (grepl("load average", line)) {
			l <- strsplit(line, " ")
			l <- l[[1]]
			cpu = as.numeric(sub(",", "", l[[12]]))

		} else if (startsWith(line, "# ")) {
			if (startsWith(line, "# actual ops")) {
				actual = actual + as.numeric(sub("# actual ops/s: ", "", line))
				clients = clients + 1
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
	ts$readp = read_perc
	ts$skew = skew
	ts$cpu = cpu

	# expected batch size when batch time 1ms
	# ops/s = (target / clients)
	# s/batch = 0.001
	# ops/batch = target * 0.001 / clients
	# let's then compute the processing time normalized to per op
	# rmt = s/batch, want s/op, so /= ops/batch => *= batch/ops
	ts$rmt_norm = (ts$rmt * clients * 1000.0) / target

	ts <- data.frame(ts)

	t <- rbind(t, ts)
	close(con)
}
#t
t = t[t$pct != 100,]
t$server <- as.factor(t$server)
t$actual <- t$actual/1000000.0
t$target <- t$target/1000000.0
t$rmt <- t$rmt/1000.0
t$sjrn <- t$sjrn/1000.0
mx_rmt = max(t[t$pct == 50,]$rmt)
mx_rmt_norm = max(t[t$pct == 50,]$rmt_norm)
t$sjrn <- pmin(t$sjrn, 10*mx_rmt) # otherwise ggplot tries to plot all the way to 100k

library(ggplot2)
p <- ggplot(data=t, aes(x=target, y=rmt, color=server, linetype=pct, shape=server))
p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, mx_rmt))
p <- p + facet_grid(skew ~ op)
p <- p + scale_linetype_manual(breaks=c(50,95,99), values=c("dotted","dashed","solid"))
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("batch processing time (ms)") + ggtitle("Batch processing time")
ggsave('plot-batch.png',plot=p,width=10,height=4)

p <- ggplot(data=t, aes(x=target, y=rmt_norm, color=server, linetype=pct, shape=server))
#p <- p + coord_trans(x = "identity", y = "log10", limy=c(1, mx_rmt_norm))
p <- p + scale_y_log10()
p <- p + facet_grid(skew ~ op)
p <- p + scale_linetype_manual(breaks=c(50,95,99), values=c("dotted","dashed","solid"))
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("op processing time (µs)") + ggtitle("Estimated per-operation latency")
ggsave('plot-op-norm.png',plot=p,width=10,height=4)

# for sojourn time, read and write should basically be the same
worst <- data.frame(t %>% group_by(target, pct, server, skew) %>% summarize(sjrn = max(sjrn)))
p <- ggplot(data=worst, aes(x=target, y=sjrn, color=server, linetype=pct, shape=server))
p <- p + coord_trans(x = "identity", y = "identity", limy=c(0, 1.2*mx_rmt))
p <- p + facet_wrap(~ skew)
p <- p + scale_linetype_manual(breaks=c(50,95,99), values=c("dotted","dashed","solid"))
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("sojourn time (ms)") + ggtitle("Sojourn time")
ggsave('plot-sjrn.png',plot=p,width=10,height=4)

t <- t[t$pct == 50,]
t <- t[t$op == "read",]
p <- ggplot(data=t, aes(x=target, y=cpu, color=server, shape=server))
p <- p + facet_wrap(~ skew)
p <- p + geom_hline(yintercept=16)
p <- p + geom_point(size = 0.7, alpha = 0.8) + geom_line()
p <- p + xlab("target load (Mops/s)") + ylab("cpu load") + ggtitle("Server CPU load")
ggsave('plot-cpu.png',plot=p,width=10,height=4)

#library(plotly)
#p <- subplot(p, p, nrows = 2, margin = 0.1)
#htmlwidgets::saveWidget(p, "plots.html", selfcontained=F)
#browseURL("plots.html")
