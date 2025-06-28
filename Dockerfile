FROM rocker/tidyverse:4.5

COPY . btt_nox_processing

LABEL org.opencontainers.image.source="https://github.com/wacl-york/btt_nox_processing"