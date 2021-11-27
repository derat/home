# collector

The `collector` daemon collects local data:

*   Sensors (e.g. Arduino boards) post readings to the `/report` HTTP endpoint
    ([listener.go](./listener.go)).
*   The daemon collects network data ([ping.go](./ping.go)).
*   The daemon optionally collects power data from a UPS
    ([power.go](./power.go)).

Data is then forwarded to the App Engine app via HTTPS
([reporter.go](./reporter.go)).
