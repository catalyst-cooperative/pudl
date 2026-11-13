# pudl.deploy.github

Helpers for triggering GitHub Actions workflows from deployment code.

## Attributes

| [`logger`](#pudl.deploy.github.logger)   |    |
|-----------------------------------------------------------|----|

## Functions

| [`dispatch_github_workflow`](#pudl.deploy.github.dispatch_github_workflow)(repo, workflow_file, ref, token) → None   | Trigger a workflow_dispatch event on a GitHub Actions workflow.   |
|---------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| [`redeploy_eel_hole`](#pudl.deploy.github.redeploy_eel_hole)(token, environment) → None                       | Redeploy Eel Hole (the PUDL data viewer) with its latest image.   |

## Module Contents

### pudl.deploy.github.logger

### pudl.deploy.github.dispatch_github_workflow(repo: [str](https://docs.python.org/3/builtins/stdtypes.html#str), workflow_file: [str](https://docs.python.org/3/builtins/stdtypes.html#str), ref: [str](https://docs.python.org/3/builtins/stdtypes.html#str), token: [str](https://docs.python.org/3/builtins/stdtypes.html#str), inputs: [dict](https://docs.python.org/3/builtins/stdtypes.html#dict)[[str](https://docs.python.org/3/builtins/stdtypes.html#str), [str](https://docs.python.org/3/builtins/stdtypes.html#str)] | [None](https://docs.python.org/3/builtins/constants.html#None) = None) → [None](https://docs.python.org/3/builtins/constants.html#None)

Trigger a workflow_dispatch event on a GitHub Actions workflow.

* **Parameters:**
  * **repo** – GitHub repo in “owner/name” form (e.g. “catalyst-cooperative/pudl”).
  * **workflow_file** – Workflow filename (e.g. “zenodo-data-release.yml”).
  * **ref** – Git branch or tag to run the workflow from.
  * **token** – Bearer token to authenticate to GitHub.
  * **inputs** – workflow_dispatch inputs, if the workflow takes any.

### pudl.deploy.github.redeploy_eel_hole(token: [str](https://docs.python.org/3/builtins/stdtypes.html#str), environment: Literal['staging', 'production']) → [None](https://docs.python.org/3/builtins/constants.html#None)

Redeploy Eel Hole (the PUDL data viewer) with its latest image.

* **Parameters:**
  * **token** – the bearer token to authenticate to GitHub.
  * **environment** – redeploy the staging or production Eel Hole.
