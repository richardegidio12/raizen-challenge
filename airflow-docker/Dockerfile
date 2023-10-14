FROM apache/airflow:2.7.0
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         libreoffice \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN chown -R airflow: ${AIRFLOW_HOME}
#RUN rm ~/.config/libreoffice/4/user/config/javasettings_Linux_*.xml
USER airflow
COPY ./requirements.txt ./
RUN pip install --upgrade pip \
    && pip install --upgrade setuptools \
    && pip install --user --no-cache-dir -r requirements.txt
WORKDIR ${AIRFLOW_HOME}