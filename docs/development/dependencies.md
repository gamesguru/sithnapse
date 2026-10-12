# Managing dependencies with uv

This is a quick cheat sheet for developers on how to use [`uv`](https://github.com/astral-sh/uv).

# Installing

See the [contributing guide](contributing_guide.md#4-install-the-dependencies).

Developers should use `uv` 0.5.0 or higher. If you encounter problems, please [double-check your uv version](#check-the-version-of-uv-with-uv---version).

# Background

Synapse uses a variety of third-party Python packages to function as a homeserver.
Some of these are direct dependencies, listed in `pyproject.toml` under the
`dependencies` section. The rest are transitive dependencies (the
things that our direct dependencies themselves depend on, and so on recursively.)

We maintain a locked list of all our dependencies (transitive included) so that
we can track exactly which version of each dependency appears in a given release.
See [here](https://github.com/matrix-org/synapse/issues/11537#issue-1074469665)
for discussion of why we wanted this for Synapse. We chose to use
[`uv`](https://github.com/astral-sh/uv) to manage this locked list; see
[this comment](https://github.com/matrix-org/synapse/issues/11537#issuecomment-1015975819)
for the reasoning.

The locked dependencies get included in our "self-contained" releases: namely,
our docker images and our debian packages. We also use the locked dependencies
in development and our continuous integration.

Separately, our "broad" dependencies—the version ranges specified in
`pyproject.toml`—are included as metadata in our "sdists" and "wheels" [uploaded
to PyPI](https://pypi.org/project/matrix-synapse). Installing from PyPI or from
the Synapse source tree directly will _not_ use the locked dependencies; instead,
they'll pull in the latest version of each package available at install time.

## Example dependency

An example may help. We have a broad dependency on
[`phonenumbers`](https://pypi.org/project/phonenumbers/), as declared in
this snippet from pyproject.toml [as of Synapse 1.156]:

```toml
dependencies = [
    # ...
    "phonenumbers>=8.2.0",
]
```

In our lockfile `uv.lock` this is pinned to a specific version.

The lockfile also includes cryptographic checksums of the sdists and wheels provided for this version.

We can see this pinned version inside the docker image for that release:

```
$ docker pull matrixdotorg/synapse:latest
...
$ docker run --entrypoint pip matrixdotorg/synapse:latest show phonenumbers
Name: phonenumbers
Version: 9.0.33
...
```

# Tooling recommendation: direnv

[`direnv`](https://direnv.net/) is a tool for activating environments in your
shell inside a given directory. We thoroughly recommend it for daily use. To use it:

1. [Install `direnv`](https://direnv.net/docs/installation.html) - it's likely
   packaged for your system already.
2. Mark the synapse checkout as a uv project by specifying the virtualenv location: `echo "layout virtualenv .venv" > .envrc`.
3. Convince yourself that you trust this `.envrc` configuration and project.
   Then formally confirm this to `direnv` by running `direnv allow`.

Then whenever you navigate to the synapse checkout, your shell commands will automatically run in the
context of the virtual environment, without having to run `source .venv/bin/activate` beforehand.


# How do I...

## ...reset my venv to the locked environment?

```shell
uv sync --all-extras --sync
```

## ...delete everything and start over from scratch?

```shell
# Stop the current virtualenv if active
# deactivate

# Remove all of the files from the current environment.
$ rm -rf .venv

# Reactivate your shell to create the virtualenv again
$ source .venv/bin/activate
# Install everything again
$ uv sync --all-extras
```

If you want to go even further and remove the uv caches, see [Clear caches](#clear-caches-uv-cache-clean).


## ...run a command in the `uv` virtualenv?

Use `uv run cmd args` when you need the python virtualenv context.
To avoid typing `uv run` all the time, you can run `source .venv/bin/activate`
to start a new shell in the uv virtualenv context. Within `source .venv/bin/activate`,
`python`, `pip`, `mypy`, `trial`, etc. are all run inside the project virtualenv
and isolated from the rest of the system.

Roughly speaking, the translation from a traditional virtualenv is:
- `env/bin/activate` -> `source .venv/bin/activate`, and
- `deactivate` -> close the terminal (Ctrl-D, `exit`, etc.)

See also the direnv recommendation above, which makes `uv run` and
`source .venv/bin/activate` unnecessary.


## ...inspect the `uv` virtualenv?

Some suggestions:

```shell
uv run pip list
```


## ...add a new dependency?

Either manually edit `pyproject.toml` or use the CLI commands.

**Using uv:**
```shell
uv add packagename
```

Include the updated `pyproject.toml` and `uv.lock` files in your commit.

## ...remove a dependency?

This is not done often and is untested, but:

**Using uv:**
```shell
uv remove packagename
```

Include the updated `pyproject.toml` and `uv.lock` files in your commit.

## ...update the version range for an existing dependency?

Best done by manually editing `pyproject.toml`, and then locking:

**Using uv:**
```shell
uv lock
```

Include the updated `pyproject.toml` and `uv.lock` in your commit.

## ...update a dependency in the locked environment?

To use the latest version of `packagename` in the locked environment, without affecting the broad dependencies listed in the wheel:

**Using uv:**
```shell
uv lock --upgrade-package packagename
```

There doesn't seem to be a way to do this whilst locking a _specific_ version of
`packagename`. We can workaround this (crudely) as follows:

```shell
uv add packagename==1.2.3
# This should update uv.lock.

# Now undo the changes to pyproject.toml. For example
# git restore pyproject.toml

# Get uv to recompute the content-hash of pyproject.toml without changing
# the locked package versions.
uv lock
```

Either way, include the updated `uv.lock` file in your commit.

## ...export a `requirements.txt` file?

```shell
uv export --all-extras --output-file requirements.txt
```

Be wary of bugs in `pip install -r requirements.txt`.

## ...build a test wheel?

I usually use

```shell
uv pip install build && uv run python -m build
```

because [`build`](https://github.com/pypa/build) is a standardish tool which
doesn't require our package manager. (It's what we use in CI too). However, you could try
`uv build` too.

## ...handle a Dependabot pull request?

Synapse uses Dependabot to keep the `uv.lock` and `Cargo.lock` files
up-to-date with the latest releases of our dependencies. The changelog check is
omitted for Dependabot PRs; the release script will include them in the 
changelog.

When reviewing a dependabot PR, ensure that:

* the lockfile changes look reasonable;
* the upstream changelog file (linked in the description) doesn't include any
  breaking changes;
* continuous integration passes.

In particular, any updates to the type hints (usually packages which start with `types-`)
should be safe to merge if linting passes.

# Troubleshooting

## Check the version of uv with `uv --version`.

The minimum version of uv supported by Synapse is 0.5.x.

## Clear caches: `uv cache clean`.

uv caches a bunch of information about packages that isn't readily available
from PyPI. Try `uv cache clean` to see if that fixes things.

## Remove outdated egg-info

Delete the `matrix_synapse.egg-info/` directory from the root of your Synapse
install.

This stores some cached information about dependencies and often conflicts with
letting uv do the right thing.



## Try `--verbose` or `--dry-run` arguments.

Sometimes useful to see what uv's internal logic is.
