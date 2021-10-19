FROM tercen/pamsoft_grid:1.0.3

ENV RENV_VERSION 0.13.2
RUN R -e "install.packages('remotes', repos = c(CRAN = 'https://cran.r-project.org'))"
RUN R -e "remotes::install_github('rstudio/renv@${RENV_VERSION}')"
RUN apt-get update && apt-get install -y icu-devtools

COPY . /operator

WORKDIR /operator

#RUN R -vanilla -e "options(HTTPUserAgent='R/4.0.4 R (4.0.4 x86_64-pc-linux-gnu x86_64 linux-gnu)');renv::consent(provided=TRUE);renv::restore(confirm=FALSE)"

RUN R -vanilla -e "renv::consent(provided=TRUE);renv::restore(confirm=FALSE)"

ENV TERCEN_SERVICE_URI https://tercen.com

ENTRYPOINT [ "R","--no-save","--no-restore","--no-environ","--slave","-f","main.R", "--args"]
CMD [ "--taskId", "someid", "--serviceUri", "https://tercen.com", "--token", "sometoken"]