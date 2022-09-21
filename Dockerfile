FROM python:3.8-slim

# pydsstools is not available in a package manager, so we need to download and install it ourselves
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends wget

RUN mkdir /opt/hydrograph_stats
WORKDIR /opt/hydrograph_stats

COPY hydrograph_stats.py requirements.txt ./
RUN pip install -r requirements.txt

# install linux version of pydsstools, may require Ubuntu 20.04 LTS and Python 3.8
RUN wget https://github.com/gyanz/pydsstools/raw/master/dist/pydsstools-2.1-cp38-cp38-linux_x86_64.whl && pip install ./pydsstools-2.1-cp38-cp38-linux_x86_64.whl

ENTRYPOINT ["./hydrograph_stats.py"]
