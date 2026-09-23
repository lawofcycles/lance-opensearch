---
name: 🐛 Bug report
about: Create a report to help us improve
title: '[BUG]'
labels: 'bug, untriaged'
assignees: ''
---

**What is the bug?**
A clear and concise description of the bug.

**How can one reproduce the bug?**
Steps to reproduce the behavior, as `curl` commands where possible:
1. Attach or register the table with '...'
2. Run the query '....'
3. See error

**What is the expected behavior?**
A clear and concise description of what you expected to happen.

**What is your host/environment?**
 - OS and CPU architecture: [e.g. linux/x86_64]
 - OpenSearch version: [e.g. 3.8.0]
 - Plugin version: [e.g. 0.1.0]
 - Lance version that wrote the table, and the table's Arrow schema (`lance.dataset(uri).schema` in Python)
 - Storage: [e.g. local filesystem, S3]
 - Other plugins installed

**Do you have any screenshots?**
If applicable, add screenshots to help explain your problem.

**Do you have any additional context?**
Add any other context about the problem, for example the mapping (`GET /<index>/_mapping`) and the plan (`GET /<index>/_lance/explain`).
