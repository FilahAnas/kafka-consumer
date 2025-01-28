FROM golang:1.23-alpine

RUN apk add --no-cache git make

WORKDIR /app

COPY go.mod go.sum Makefile ./

ARG GITHUB_TOKEN

RUN git config --global url."https://${GITHUB_TOKEN}:@github.com/".insteadOf "https://github.com/"

COPY . .

CMD ["make", "run"]
