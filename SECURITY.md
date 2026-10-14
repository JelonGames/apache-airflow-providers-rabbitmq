# Security Policy

## Supported versions

Only the latest major release receives security fixes. Older majors get a fix only when the upgrade path to the latest is not practical.

| Version | Supported |
| ------- | --------- |
| 5.x     | Yes       |
| < 5.0   | No        |

## Reporting a vulnerability

Do not open a public issue for a security problem. Email mustafa@zidan.me with:

- the provider, Airflow and Python versions affected,
- a description of the problem and what an attacker can do with it,
- steps or a snippet that reproduce it.

You should get an acknowledgement within a week. Once the problem is confirmed, a fix is released and a GitHub security advisory is published, crediting the reporter unless they ask not to be named.

Connection credentials are a common source of reports for this provider. A password or URI showing up in task logs, XCom or rendered templates counts as a vulnerability and should be reported this way.
