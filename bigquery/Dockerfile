FROM alpine:3.18

RUN apk add --no-cache curl bash

COPY init-bigquery.sh /init-bigquery.sh
RUN chmod +x /init-bigquery.sh

CMD ["/init-bigquery.sh"]