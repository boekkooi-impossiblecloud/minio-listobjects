FROM minio/minio:RELEASE.2024-03-21T23-13-43Z

COPY ./minio /usr/bin/minio
#COPY dockerscripts/docker-entrypoint.sh /usr/bin/docker-entrypoint.sh

#ENTRYPOINT ["/usr/bin/docker-entrypoint.sh"]

#VOLUME ["/data"]

CMD ["minio"]
