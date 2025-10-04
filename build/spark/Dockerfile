FROM bitnami/spark:3.5

# Optional
USER root
RUN install_packages curl ca-certificates && update-ca-certificates
USER 1001

# Arbeitsverzeichnis
WORKDIR /opt/spark-app

# Jobs + Submit-Script rein
COPY jobs/ ./jobs/
COPY submit.sh ./submit.sh
RUN chmod +x ./submit.sh

# Standard-Command: startet nur die Bronze-Ingestion
CMD ["/opt/spark-app/submit.sh"]
