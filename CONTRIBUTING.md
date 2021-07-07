<!---
Licensed under Creative Commons Attribution 4.0 International License
https://creativecommons.org/licenses/by/4.0/
--->
## Contributing

We appreciate your contributions to MirBFT.
Any help is welcome and there is still much to do. 

We adopt Fabric's developer guidelines where possible. Please visit Fabric's [contributors guide](http://hyperledger-fabric.readthedocs.io/en/latest/CONTRIBUTING.html) in the
docs to learn more about it.

Note that we follow the Hyperledger [Charter](https://www.hyperledger.org/about/charter), particularly,
all new inbound code contributions to Fabric Private Chaincode shall be made under the Apache License, Version 2.0.
All outbound code will be made available under the Apache License, Version 2.0.

### A brief contribution guide

#### Everything starts with an issue

Contributions to MirBFT usually start by creating a [new issue](https://github.com/hyperledger-labs/mirbft/issues) to report a bug or suggest a new feature.
If you are reporting a bug, please try to provide enough information to allow someone else to reproduce the bug.
For instance, tell us which version of the code (i.e., which commit or release) you are using, etc. 
If you are proposing a new feature or enhancement, please describe the problem you are trying to solve with enough details.
Explain the solution you have in mind. Try to formulate a clear and concise description of what you want to happen. Have you considered any alternatives?
Next, it is time to trigger a discussion on your new issue and to make this an interactive process to reach agreement on a solution. Feel free to post your issue also on #mirbft Rocket Chat and asked for feedback.

#### Let's code

There are many guidelines online available describing how to contribute to a github project, in particular, how to fork,
clone, and configure your local repository with origin and upstream targets. These online guides / tutorials also show
that there are many ways to get the job done. Most of the time it is personal taste and experience.
We recommend reading the [Fabric Private Chaincode Git Flow](https://docs.google.com/document/d/1sR7YV3pSYN3NEFiW-2fUqtpsJeJrpC0EWUVtEm0Blcg/edit#heading=h.kwcug3pkefak) documentation, which provides a step-by-step tutorial how to setup your git project and create a push request (PR).
This document is a good starting point for beginners but also a good refresher for experienced developers (git users).

When starting working on a new feature or bug fix, create a new branch based on the most recent `main` branch.
Now, hack the solution as described in the corresponding issue. Ideally your new code comes with some unit tests to show that it is working as expected. Please follow programming best practices.
Try to keep commits small. Each commit should have a concise title, and a detailed description of the contents.

#### Creating a PR and the review process

Once you are happy with your solution, push your branch to your origin (fork) and create a new pull request (PR) on [github](https://github.com/hyperledger-labs/mirbft/pulls).
Link your PR to the corresponding issue and provide additional information (if necessary) that are helpful to review this PR. 
Make sure that your PR passes all github checks, in particular, that the DCO check has passed, and the testing pipeline has succeeded. 
Next, a maintainer will review your PR or assign a reviewer. The review process is intended to be an interactive process where you work together with the reviewers to improve your PR before it is eventually merged.
That is, a reviewer may request to revise your PR to ensure high code quality and functionality. Finally, the reviewer will approve your PR and mark it thereby to be merged by a maintainer.
To keep the git history clean, we prefer to squash commits and rebase them before merging. 

We hope this brief contribution guide gives you a starting point for your first contribution.
If you have any questions and suggestions, do not hesitate to contact us.
Join us on [RocketChat](https://chat.hyperledger.org/) in #mirbft.

<a rel="license" href="http://creativecommons.org/licenses/by/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by/4.0/88x31.png" /></a><br />This work is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.
