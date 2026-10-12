# Application auth

`GET /v1/heads/ethereum` without credentials is 401. The same path with a wrong bearer is 401. `scripts/doctor.sh` and `scripts/e2e.sh` check both.
