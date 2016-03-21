library(ggplot2)
library(reshape)

nmachines <- 16
bs <- 1024

flops <- function(solver, n, d, k, sparsity) {
  ifelse(solver=="LS - LBFGS",
  20*n*sparsity*d*k/nmachines,
  ifelse(solver=="Exact",
  n*d*(d+k)/nmachines,
  3*(n*d*(bs+k)/nmachines)))
}

mem <- function(solver, n, d, k, sparsity) {
  ifelse(solver=="LS - LBFGS",
    20*n*d*sparsity/nmachines,
    ifelse(solver=="Exact",
    n*d/nmachines + d**2,
    3*(n*d/nmachines + d*k)))
}

network <- function(solver, n, d, k, sparsity) {
  ifelse(solver=="LS - LBFGS",
    20*2*d*k*log(nmachines),
    ifelse(solver=="Exact",
    d*(d+k),
    3*2*(d*(bs+k))*log(nmachines)))
}


main <- function () {
  x <- read.csv("solver-comparisons-final.csv")
  n=list(Amazon=65e6, TIMIT=2.2e6)
  k=list(Amazon=2, TIMIT=138)
  nnz=list(Amazon=0.005, TIMIT=1.0)
  colnames(x) <- c("Experiment", "solver", "d","t","train.error","loss","loss.2")


  x$n <- as.numeric(n[x$Experiment])
  x$k <- as.numeric(k[x$Experiment])
  x$s <- as.numeric(nnz[x$Experiment])

  x$cpu <- with(x, flops(solver, n, d, k, s))
  x$mem <- with(x, mem(solver, n, d, k, s))
  x$network <- with(x, network(solver, n, d, k, s))

  list(data=x, model=lm(t ~ cpu + mem + network, data=x))
}


plotter <- function(res) {
  res$data$pred <- predict(res$model, res$data)
  dn <- res$data[,c("Experiment","solver","d","t","pred")]
  dnm <- melt(dn, id.vars=c("Experiment","solver","d"))

  qplot(d, value, geom='line', color=solver, shape=variable, data=dnm) +
    facet_grid(Experiment ~ ., scale="free_y") +
    theme_bw() +
    geom_point()
}