FROM rocker/r-ver:4.5

COPY . btt_nox_processing

RUN Rscript btt_nox_processing/docker/install.R

LABEL org.opencontainers.image.source="https://github.com/wacl-york/btt_nox_processing"