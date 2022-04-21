FROM python:3.9.12-slim-bullseye

RUN apt-get update
RUN apt-get install -y curl

RUN mkdir /opt/hydrograph_stats
WORKDIR /opt/hydrograph_stats

COPY hydrograph_stats.py requirements.txt ./
RUN pip install -r requirements.txt

ENTRYPOINT ["./hydrograph_stats.py"]
