# AGENTS.md

## Code Structure

This is a CLI, build using python with `uv` and `typer`.

- Main `uv` docs: https://docs.astral.sh/uv/
- Main `typer` docs: https://typer.tiangolo.com/

`rich` is used for printing/styling/terminal output:

- Main `rich` docs: https://rich.readthedocs.io/en/latest/index.html

All of the code should be in `src/deweypy` and the top level package is `deweypy`.

`deweypy` (and `dewey`) stands for and relates to Dewey Data, the company that owns and
develops this project (https://www.deweydata.io).

### CLI (Typer)

For Typer, please use the latest best practices from the latest version of `typer`. If
you're unsure, ask and/or read the documentation first.

### Running/Testing the CLI

When testing CLI commands, use `python -m deweypy ...` within the activated virtual
environment rather than just `dewey ...`. This ensures you're running the local
development version and not a globally installed `dewey` command (see `pyproject.toml`
for why the `deweypy` and `dewey` difference) that might exist on the system. For
example:

```bash
source .venv/bin/activate
python -m deweypy hello
python -m deweypy --help
```

Note: The `dewey` command does work after `uv sync`, but `python -m deweypy` is
preferred for development and testing to avoid ambiguity.

## Taskfile

We use `Taskfile.yml` from https://taskfile.dev/ for commands to run.

- Main `task` (`go-task`) docs: https://taskfile.dev/docs/guide

You can read the `Taskfile.yml` to see some common commands. If you're unsure how to do
something within this project that's a great spot to check before asking.

## Environment

To activate Python, run `source .venv/bin/activate` to use this virtual environment. If
you're on Windows, you might need to do `.venv\Scripts\activate.bat`, but prefer the
Linux/Mac first and fall back to Windows if you know you're on Windows or the other one
doesn't work.

## Code Style

### Comments

For code comments, please try and use full sentences unless it's like a one or two
(e.g.) summary type of comment block, or the scenario really merits not using a full
sentence.

Also, for code comments, please, if referencing or commenting on variables, enclose them
with backticks, e.g. Set `some_var` to the result of, ..., etc.

### Printing and Logging

For now, please do `from rich import print as rprint` and use `rprint` for everything.
Don't print unecessarily; but rather just the main important stuff. Don't use logging
either.

## Other Documentation to Reference and Update

**Important**: Before doing anything, read everything in `docs/agent-notes/**`.

If your change is significant to add to that, make a new file (probably Markdown but you
can make more files if you want to reference additional files). Title it `YYYY-MM-DD
Some Descriptive Title.md` (or different file extension). From there, even though
redundant, put a top level section that indicates the time the change was made, what
`commit` and `branch` you were on (and worktree) when starting to make the change, what
time the change was made, and what you want to note/document. Try and keep these notes
high level and to the point, but leave enough information for future agents and human
engineers to understand what needs to be understood.
