# Security

Report security issues privately to the project maintainers. Do not open a
public issue for credentials, SSRF, auth bypass, data exfiltration, or
deployment-hardening findings.

Collection is designed to run with separate database roles:

- `gordios_ingester`: writes raw ingestion tables.
- `gordios_raw_gateway`: reads raw tables and writes ingestion AOI metadata.

The Docker passwords in `docker/init/00-local-roles.sql` are for local testing
only. Production deployments must create their own credentials.
