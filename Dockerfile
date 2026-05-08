FROM scratch

ARG TARGETPLATFORM

COPY ./dist/${TARGETPLATFORM} /cache_cat

ENTRYPOINT ["/cache_cat"]