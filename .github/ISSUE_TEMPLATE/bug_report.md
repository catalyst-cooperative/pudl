---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

### Describe the bug
A clear and concise description of what the bug is.

### Bug Severity
How badly is this bug affecting you?
 - **High:** This bug is preventing me from using PUDL.
 - **Medium:** With some effort, I can work around the bug.
 - **Low:** The bug isn't causing me problems, but something's still wrong here.

### To Reproduce
Steps to reproduce the behavior -- ideally including a code snippet that causes the error to appear.
 - Is the bug related to the software / database? If so, please attach the `settings.yml` file you're using to specify which data to load, and make a note of where in the ETL process the error is happening.
 - Have you found an error or inconsistency in the data that PUDL brings together? If so, what is the data source, year, plant_id, etc -- how can we find the data you're looking at, and what is the nature of the error or inconsistency.
 - Does the problem happen when you're starting up the database (importing data) or does it happen later when you're trying to use the database?

### Expected behavior
A clear and concise description of what you expected to happen, or what you expected the data to look like.

### Software Environment?
 - Operating System. (e.g. MacOS 14.5, Ubuntu 22.04, Windows Subsystem for Linux v2)
 - Python version and distribution (e.g. Anaconda Python 3.10.6)
 - How did you install PUDL?
    - If you installed using `git clone` what branch are you using (probably `main` or `dev`)
    - If you installed using `pip`, `conda` or `mamba` what version did you install?

### Additional context
Add any other context about the problem here.
