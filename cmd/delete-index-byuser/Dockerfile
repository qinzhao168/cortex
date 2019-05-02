FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       delete-index-byuser /bin/delete-index-byuser
EXPOSE     80
ENTRYPOINT [ "/bin/delete-index-byuser" ]

ARG revision
LABEL org.opencontainers.image.title="delete-index-byuser" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/delete-index-byuser" \
      org.opencontainers.image.revision="${revision}"
