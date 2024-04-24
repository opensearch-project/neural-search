# Contributing Guidelines

Thank you for your interest in contributing to our project. Whether it's a bug report, new feature, correction, or additional
documentation, we greatly value feedback and contributions from our community.

Please read through this document before submitting any issues or pull requests to ensure we have all the necessary
information to effectively respond to your bug report or contribution.


## Reporting Bugs/Feature Requests

We welcome you to use the GitHub issue tracker to report bugs or suggest features.

When filing an issue, please check existing open, or recently closed, issues to make sure somebody else hasn't already
reported the issue. Please try to include as much information as you can. Details like these are incredibly useful:

* A reproducible test case or series of steps
* The version of our code being used
* Any modifications you've made relevant to the bug
* Anything unusual about your environment or deployment


## Contributing via Pull Requests
Contributions via pull requests are much appreciated. Before sending us a pull request, please ensure that:

1. You are working against the latest source on the *main* branch.
2. You check existing open, and recently merged, pull requests to make sure someone else hasn't addressed the problem already.
3. You open an issue to discuss any significant work - we would hate for your time to be wasted.

To send us a pull request, please:

1. Fork the repository.
2. Modify the source; please focus on the specific change you are contributing. If you also reformat all the code, it will be hard for us to focus on your change.
3. Include tests that check your new feature or bug fix. Ideally, we're looking for unit, integration, and BWC tests, but that depends on how big and critical your change is.
If you're adding an integration test and it is using local ML models, please make sure that the number of model deployments is limited, and you're using the smallest possible model.
Each model deployment consumes resources, and having too many models may cause unexpected test failures.
4. Ensure local tests pass.
5. Commit to your fork using clear commit messages.
6. Send us a pull request, answering any default questions in the pull request interface.
7. Pay attention to any automated CI failures reported in the pull request, and stay involved in the conversation.

GitHub provides additional document on [forking a repository](https://help.github.com/articles/fork-a-repo/) and
[creating a pull request](https://help.github.com/articles/creating-a-pull-request/).

## Changelog

OpenSearch maintains version specific changelog by enforcing a change to the ongoing [CHANGELOG](CHANGELOG.md) file adhering to the [Keep A Changelog](https://keepachangelog.com/en/1.0.0/) format. The purpose of the changelog is for the contributors and maintainers to incrementally build the release notes throughout the development process to avoid a painful and error-prone process of attempting to compile the release notes at release time. On each release the "unreleased" entries of the changelog are moved to the appropriate release notes document in the `./release-notes` folder. Also, incrementally building the changelog provides a concise, human-readable list of significant features that have been added to the unreleased version under development.

### Which changes require a CHANGELOG entry?
Changelogs are intended for operators/administrators, developers integrating with libraries and APIs, and end-users interacting with OpenSearch Dashboards and/or the REST API (collectively referred to as "user"). In short, any change that a user of OpenSearch might want to be aware of should be included in the changelog. The changelog is _not_ intended to replace the git commit log that developers of OpenSearch itself rely upon. The following are some examples of changes that should be in the changelog:

- A newly added feature
- A fix for a user-facing bug
- Dependency updates
- Fixes for security issues

The following are some examples where a changelog entry is not necessary:

- Adding, modifying, or fixing tests
- An incremental PR for a larger feature (such features should include _one_ changelog entry for the feature)
- Documentation changes or code refactoring
- Build-related changes

Any PR that does not include a changelog entry will result in a failure of the validation workflow in GitHub. If the contributor and maintainers agree that no changelog entry is required, then the `skip-changelog` label can be applied to the PR which will result in the workflow passing.

### How to add my changes to [CHANGELOG](CHANGELOG.md)?

Adding in the change is two step process:
1. Add your changes to the corresponding section within the CHANGELOG file with dummy pull request information, publish the PR
2. Update the entry for your change in [`CHANGELOG.md`](CHANGELOG.md) and make sure that you reference the pull request there.

### Where should I put my CHANGELOG entry?
Please review the [branching strategy](https://github.com/opensearch-project/.github/blob/main/RELEASING.md#opensearch-branching) document. The changelog on the `main` branch will contain sections for the _next major_ and _next minor_ releases. Your entry should go into the section it is intended to be released in. In practice, most changes to `main` will be backported to the next minor release so most entries will likely be in that section.

The following examples assume the _next major_ release on main is 3.0, then _next minor_ release is 2.5, and the _current_ release is 2.4.

- **Add a new feature to release in next minor:** Add a changelog entry to `[Unreleased 2.x]` on main, then backport to 2.x (including the changelog entry).
- **Introduce a breaking API change to release in next major:** Add a changelog entry to `[Unreleased 3.0]` on main, do not backport.
- **Upgrade a dependency to fix a CVE:** Add a changelog entry to `[Unreleased 2.x]` on main, then backport to 2.x (including the changelog entry), then backport to 2.4 and ensure the changelog entry is added to `[Unreleased 2.4.1]`.


## Finding contributions to work on
Looking at the existing issues is a great way to find something to contribute on. As our projects, by default, use the default GitHub issue labels (enhancement/bug/duplicate/help wanted/invalid/question/wontfix), looking at any 'help wanted' issues is a great place to start.


## Code of Conduct
This project has adopted the [Amazon Open Source Code of Conduct](https://aws.github.io/code-of-conduct).
For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq) or contact
opensource-codeofconduct@amazon.com with any additional questions or comments.


## Security issue notifications
If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public github issue.


## Licensing

See the [LICENSE](LICENSE) file for our project's licensing. We will ask you to confirm the licensing of your contribution.
