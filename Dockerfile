FROM python:3.9.12-slim-bullseye

RUN mkdir /opt/hydrograph_stats
WORKDIR /opt/hydrograph_stats

COPY hydrograph_stats.py requirements.txt ./
RUN pip install -r requirements.txt

ENTRYPOINT ["./hydrograph_stats.py"]
