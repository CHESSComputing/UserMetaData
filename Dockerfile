FROM golang:latest AS go-builder

# build procedure
ENV PROJECT=usermetadata
ENV WDIR=/data
WORKDIR $WDIR
RUN mkdir /build
RUN git clone https://github.com/CHESSComputing/$PROJECT
ARG CGO_ENABLED=0
RUN cd $PROJECT && make && cp srv /build

# build final image for given image
FROM alpine AS final
# FROM gcr.io/distroless/static AS final
RUN mkdir -p /data
COPY --from=go-builder /build/srv /data
LABEL org.opencontainers.image.authors="CHESS Computing group"
LABEL org.opencontainers.image.description="FOXDEN usermetadata service"
LABEL org.opencontainers.image.source=https://github.com/chesscomputing/usermetadata
LABEL org.opencontainers.image.licenses=MIT
WORKDIR /data
