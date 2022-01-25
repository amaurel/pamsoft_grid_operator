FROM debian:bullseye-slim as builder

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get -q update && \
    apt-get install -q -y --no-install-recommends \
      libtiff-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


FROM tercen/pamsoft_grid:1.0.24

COPY --from=builder /usr/include/x86_64-linux-gnu/tiff* /usr/include/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libtiff* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libtiff* /usr/lib/x86_64-linux-gnu/

ENV RENV_VERSION 0.13.2
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cran.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"


COPY . /operator

WORKDIR /operator

RUN R -e "options(HTTPUserAgent='R/4.0.4 R (4.0.4 debian:bullseye-slim x86_64 linux-gnu)');renv::consent(provided=TRUE);renv::restore(confirm=FALSE)"

ENV TERCEN_SERVICE_URI https://tercen.com

ENTRYPOINT [ "R","--no-save","--no-restore","--no-environ","--slave","-f","main.R", "--args"]
CMD [ "--taskId", "someid", "--serviceUri", "https://tercen.com", "--token", "sometoken"]