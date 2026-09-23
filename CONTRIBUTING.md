Contributing to OpenSearch Lance
================================

OpenSearch Lance is a community project that is built and maintained by people just like **you**. We're glad you're interested in helping out. There are several different ways you can do it, but before we talk about that, let's talk about how to get started.

## Table of Contents:
- [First Things First](#first-things-first)
- [Ways to Contribute](#ways-to-contribute)
- [Developer Certificate of Origin](#developer-certificate-of-origin)
- [License Headers](#license-headers)
- [Changelog](#changelog)
- [Pull Request Template](#pull-request-template)
- [Review Process](#review-process)
- [Backports](#backports)


## First Things First

1. **When in doubt, open an issue** - For almost any type of contribution, the first step is opening an issue. Even if you think you already know what the solution is, writing down a description of the problem you're trying to solve will help everyone get context when they review your pull request. If it's truly a trivial change (e.g. spelling error), you can skip this step -- but as the subject says, when in doubt, [open an issue](https://github.com/lawofcycles/lance-opensearch/issues).

2. **Only submit your own work** (or work you have sufficient rights to submit) - Please make sure that any code or documentation you submit is your work or you have the rights to submit. We respect the intellectual property rights of others, and as part of contributing, we'll ask you to sign your contribution with a "Developer Certificate of Origin" (DCO) that states you have the rights to submit this work and you understand we'll use your contribution. There's more information about this topic in the [DCO section](#developer-certificate-of-origin).

## Ways to Contribute

### Bug Reports

Ugh! Bugs!

A bug is when software behaves in a way that you didn't expect and the developer didn't intend. To help us understand what's going on, we first want to make sure you're working from the latest version. Please make sure you're testing against the [latest version](https://github.com/lawofcycles/lance-opensearch).

Once you've confirmed that the bug still exists in the latest version, you'll want to check to make sure it's not something we already know about on the [open issues GitHub page](https://github.com/lawofcycles/lance-opensearch/issues).

If you've upgraded to the latest version and you can't find it in our open issues list, then you'll need to tell us how to reproduce it. To make the behavior as clear as possible, please provide your steps as `curl` commands which we can copy and paste into a terminal to run it locally, for example:

```sh
# attach the Lance table
curl -X POST localhost:9200/_lance/attach -H 'Content-Type: application/json' -d '{
 "table": "/tables/demo.lance"
}'

# this should return XXXX but instead returns YYYY
curl ....
```

Because this plugin reads Lance tables, please also include the Arrow schema of the table (`lance.dataset(uri).schema` in Python) and the Lance version that wrote it. Provide as much information as you can. You may think that the problem lies with your query, when actually it depends on how the table is laid out. The easier it is for us to recreate your problem, the faster it is likely to be fixed.

### Feature Requests

If you've thought of a way that OpenSearch Lance could be better, we want to hear about it. We track feature requests using GitHub, so please feel free to open an issue which describes the feature you would like to see, why you need it, and how it should work.

### Documentation Changes

The user facing documentation lives under [`docs/`](docs/) in this repository. Corrections and additions there follow the same pull request process as code.

### Contributing Code

As with other types of contributions, the first step is to [**open an issue on GitHub**](https://github.com/lawofcycles/lance-opensearch/issues/new/choose). Opening an issue before you make changes makes sure that someone else isn't already working on that particular problem. It also lets us all work together to find the right approach before you spend a bunch of time on a PR. So again, when in doubt, open an issue.

Once you've opened an issue, check out our [Developer Guide](./DEVELOPER_GUIDE.md) for instructions on how to get started.

## Developer Certificate of Origin

OpenSearch Lance is an open source product released under the Apache 2.0 license (see either [the Apache site](https://www.apache.org/licenses/LICENSE-2.0) or the [LICENSE.txt file](./LICENSE.txt)). The Apache 2.0 license allows you to freely use, modify, distribute, and sell your own products that include Apache 2.0 licensed software.

We respect intellectual property rights of others and we want to make sure all incoming contributions are correctly attributed and licensed. A Developer Certificate of Origin (DCO) is a lightweight mechanism to do that.

The DCO is a declaration attached to every contribution made by every developer. In the commit message of the contribution, the developer simply adds a `Signed-off-by` statement and thereby agrees to the DCO, which you can find below or at [DeveloperCertificate.org](http://developercertificate.org/).

```
Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the
    best of my knowledge, is covered under an appropriate open
    source license and I have the right under that license to
    submit that work with modifications, whether created in whole
    or in part by me, under the same open source license (unless
    I am permitted to submit under a different license), as
    Indicated in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including
    all personal information I submit with it, including my
    sign-off) is maintained indefinitely and may be redistributed
    consistent with this project or the open source license(s)
    involved.
 ```
We require that every contribution to OpenSearch Lance is signed with a Developer Certificate of Origin. Additionally, please use your real name. We do not accept anonymous contributors nor those utilizing pseudonyms.

Each commit must include a DCO which looks like this

```
Signed-off-by: Jane Smith <jane.smith@email.com>
```
You may type this line on your own when writing your commit messages. However, if your user.name and user.email are set in your git configs, you can use `git commit -s` (or `--signoff`) to add the `Signed-off-by` line to the end of the commit message.

## License Headers

New files in your code contributions should contain the following license header. If you are modifying existing files with license headers, or including new files that already have license headers, do not remove or modify them without guidance. The `licenseHeaders` precommit task (part of `./gradlew build`) fails the build on a Java source file that lacks one.

### Java

```
/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
```

### Python

```
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
```

### Shell

```
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
```

## Changelog

This repository keeps a version specific changelog in [CHANGELOG.md](CHANGELOG.md), following the [Keep A Changelog](https://keepachangelog.com/en/1.0.0/) format. The purpose of the changelog is for the contributors and maintainers to incrementally build the release notes throughout the development process to avoid a painful and error-prone process of attempting to compile the release notes at release time. On each release the unreleased entries are assembled into a new release block of `CHANGELOG.md` and copied into the release notes document under [`release-notes/`](release-notes/).

### Which changes require a changelog entry?

Changelogs are intended for operators/administrators, developers integrating with the plugin, and end-users interacting with the REST API (collectively referred to as "user"). In short, any change that a user of the plugin might want to be aware of should be included in the changelog. The changelog is _not_ intended to replace the git commit log that developers of the plugin itself rely upon. The following are some examples of changes that should be in the changelog:

- A newly added feature
- A fix for a user-facing bug
- Dependency updates
- Fixes for security issues

The following are some examples where a changelog entry is not necessary:

- Adding, modifying, or fixing tests
- An incremental PR for a larger feature (such features should include _one_ changelog entry for the feature)
- Documentation typo fixes or code refactoring
- Build-related changes

### How to add my changes to the changelog?

Do **not** edit `CHANGELOG.md` directly. Every pull request records its entry as a fragment file instead, so that parallel pull requests do not conflict on the same lines. The rules are in [`changelog/unreleased/README.md`](changelog/unreleased/README.md); in short:

1. Create `changelog/unreleased/<section>/<topic>.md`, where `<section>` is one of `features`, `changed`, `fixed`, `infrastructure`, `documentation`, `maintenance`, `testing` and `<topic>` is a short name for your change (usually your branch name).
2. Write one or more markdown list items in the file in the style of the existing `CHANGELOG.md` entries: what changed, why, and what a user observes. Put the issue reference at the end of the item as `(#N)`.
3. Commit the fragment together with your change.

A pull request that does not add a fragment fails the `changelog` workflow. If the contributor and maintainers agree that no changelog entry is required, apply the `skip-changelog` label to the pull request, or put a line reading only `skip-changelog` in its description, and the workflow passes.

## Pull Request Template

The pull request description follows [`.github/PULL_REQUEST_TEMPLATE.md`](.github/PULL_REQUEST_TEMPLATE.md). Fill every section: the issue the pull request closes, what changes and why, the files outside the issue's scope, the hot files touched, what you verified (the commands you ran and their results, with numbers) and what you could not verify. The template also carries the organization's check list; tick the items that apply.

## Review Process

We deeply appreciate everyone who takes the time to make a contribution. We will review all contributions as quickly as possible. As a reminder, [opening an issue](https://github.com/lawofcycles/lance-opensearch/issues/new/choose) discussing your change before you make it is the best way to smooth the PR process. This will prevent a rejection because someone else is already working on the problem, or because the solution is incompatible with the architectural direction.

During the PR process, expect that there will be some back-and-forth. Please try to respond to comments in a timely fashion, and if you don't wish to continue with the PR, let us know. If a PR takes too many iterations for its complexity or size, we may reject it. Additionally, if you stop responding we may close the PR as abandoned. In either case, if you feel this was done in error, please add a comment on the PR.

If we accept the PR, we will merge your change and usually take care of backporting it to appropriate branches ourselves.

If we reject the PR, we will close the pull request with a comment explaining why. This decision isn't always final: if you feel we have misunderstood your intended change or otherwise think that we should reconsider then please continue the conversation with a comment on the PR and we'll do our best to address any further points you raise.

## Backports

This repository has not yet been submitted to the opensearch-project organization and has no release branches; every change goes to `main` and there is nothing to backport. Once release branches exist, the organization's backport workflow will create backport pull requests automatically for merged pull requests that carry a `backport <branch>` label, and this section will describe it. See [RELEASING.md](RELEASING.md) for the steps that enable it.
