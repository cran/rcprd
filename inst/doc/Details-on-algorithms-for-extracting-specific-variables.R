## ----include = FALSE----------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup--------------------------------------------------------------------
library(rcprd)

## -----------------------------------------------------------------------------
list.files(system.file("codelists", package = "rcprd"))

## ----echo = FALSE-------------------------------------------------------------
lookups.cholhdl.ratio <- read.csv("identify_numunits_cholhdl_ratio.csv")[,-1]
knitr::kable(lookups.cholhdl.ratio, caption = "Unit measurements for cholesterol/high-density lipoprotein ratio")

## ----echo = FALSE-------------------------------------------------------------
lookups.chol <- read.csv("identify_numunits_chol.csv")[,-1]
knitr::kable(lookups.chol, caption = "Unit measurements for total cholesterol")

## ----echo = FALSE-------------------------------------------------------------
lookups.hdl <- read.csv("identify_numunits_hdl.csv")[,-1]
knitr::kable(lookups.hdl, caption = "Unit measurements for high-density lipoprotein")

## ----echo = FALSE-------------------------------------------------------------
lookups.bmi <- read.csv("identify_numunits_bmi.csv")[,-1]
knitr::kable(lookups.bmi, caption = "Unit measurements for body mass index")

## ----echo = FALSE-------------------------------------------------------------
lookups.weight <- read.csv("identify_numunits_weight.csv")[,-1]
knitr::kable(lookups.weight, caption = "Unit measurements for weight")

## ----echo = FALSE-------------------------------------------------------------
lookups.height <- read.csv("identify_numunits_height.csv")[,-1]
knitr::kable(lookups.height, caption = "Unit measurements for height")

## ----echo = FALSE-------------------------------------------------------------
lookups.sbp <- read.csv("identify_numunits_sbp.csv")[,-1]
knitr::kable(lookups.sbp, caption = "Unit measurements for systolic blood pressure")

