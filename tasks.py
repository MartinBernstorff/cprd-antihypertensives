from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from invoke import Context, Result, task


def echo_header(msg: str):
    print(f"\n--- {msg} ---")


@dataclass
class Emo:
    DO = "🤖"
    GOOD = "✅"
    FAIL = "🚨"
    WARN = "🚧"
    SYNC = "🚂"
    PY = "🐍"
    CLEAN = "🧹"
    TEST = "🧪"
    COMMUNICATE = "📣"


def git_init(c: Context):
    # If no .git directory exits
    if not Path(".git").exists():
        echo_header(f"{Emo.DO} Initializing Git repository")
        c.run("git init")
        c.run("git add .")
        c.run("git commit -m 'Initial commit'")
        print(f"{Emo.GOOD} Git repository initialized")
    else:
        print(f"{Emo.GOOD} Git repository already initialized")


def check_conda(
    c: Context,
):
    # Check if in conda environment
    if "CONDA_PREFIX" in c.run("env", hide=True).stdout:
        print(f"{Emo.GOOD} In conda environment")
    else:
        print(f"{Emo.FAIL} Not in conda environment. Exiting.")
        exit(1)


def _add_commit(c: Context, msg: Optional[str] = None):
    print("🔨 Adding and committing changes")
    c.run("git add .")

    if msg is None:
        msg = input("Commit message: ")

    c.run(f'git commit -m "{msg}"', pty=True, hide=True)
    print("\n🤖 Changes added and committed\n")


def is_uncommitted_changes(c: Context) -> bool:
    git_status_result: Result = c.run(
        "git status --porcelain",
        pty=True,
        hide=True,
    )

    uncommitted_changes = git_status_result.stdout != ""
    return uncommitted_changes


def add_and_commit(c: Context, msg: Optional[str] = None, prompt: bool = True):
    """Add and commit all changes."""
    if is_uncommitted_changes(c):
        uncommitted_changes_descr = c.run(
            "git status --porcelain",
            pty=True,
            hide=True,
        ).stdout

        echo_header(
            f"{Emo.WARN} Uncommitted changes detected",
        )

        if prompt:
            input("Press enter to add and commit the changes...")

        for line in uncommitted_changes_descr.splitlines():
            print(f"    {line.strip()}")
        print("\n")
        _add_commit(c, msg=msg)


def branch_exists_on_remote(c: Context) -> bool:
    branch_name = Path(".git/HEAD").read_text().split("/")[-1].strip()

    branch_exists_result: Result = c.run(
        f"git ls-remote --heads origin {branch_name}",
        hide=True,
    )

    return branch_name in branch_exists_result.stdout


def update_branch(c: Context):
    echo_header(f"{Emo.SYNC} Syncing branch with remote")

    if not branch_exists_on_remote(c):
        c.run("git push --set-upstream origin HEAD")
    else:
        print("Pulling")
        c.run("git pull")
        print("Pushing")
        c.run("git push")


def create_pr(c: Context):
    c.run(
        "gh/usr/bin/gh pr create --web",
        pty=True,
    )


def update_pr(c: Context):
    echo_header(f"{Emo.COMMUNICATE} Syncing PR")
    # Get current branch name
    branch_name = Path(".git/HEAD").read_text().split("/")[-1].strip()
    pr_result: Result = c.run(
        "gh/usr/bin/gh pr list --state OPEN",
        pty=False,
        hide=True,
    )

    if branch_name not in pr_result.stdout:
        create_pr(c)
    else:
        open_web = input("Open in browser? [y/n] ")
        if "y" in open_web.lower():
            c.run("gh/usr/bin/gh pr view --web", pty=True)


def exit_if_error_in_stdout(result: Result):
    if "error" in result.stdout:
        exit(0)


def pre_commit(c: Context):
    """Run pre-commit checks."""

    # Essential to have a clean working directory before pre-commit to avoid committing
    # heterogenous files under a "style: linting" commit
    if is_uncommitted_changes(c):
        add_and_commit = input(
            f"{Emo.WARN} Your git working directory is not clean. Do you want to add all and commit now? [y/n]",
        )
        if "y" in add_and_commit.lower():
            _add_commit(c)
        else:
            exit(0)

    echo_header(f"{Emo.CLEAN} Running pre-commit checks")
    pre_commit_cmd = "pre-commit run --all-files"
    result = c.run(pre_commit_cmd, pty=True, warn=True)

    exit_if_error_in_stdout(result)

    if "fixed" in result.stdout or "reformatted" in result.stdout:
        _add_commit(c, msg="style: linting")

        print(f"{Emo.DO} Fixed errors, re-running pre-commit checks")
        second_result = c.run(pre_commit_cmd, pty=True, warn=True)
        exit_if_error_in_stdout(second_result)


def mypy(c: Context):
    echo_header(f"{Emo.CLEAN} Running mypy")
    c.run("mypy .", pty=True)
    
@task
def commit(c: Context, prompt: bool = False):
    add_and_commit(c, prompt=prompt)

@task
def branch(c: Context):
    new_branch_name = input("🌲 Branching from origin/main. New branch name: ")
    c.run(f"git checkout -b {new_branch_name} origin/main")


@task
def install(c: Context):
    check_conda(c)
    echo_header(f"{Emo.DO} Installing project")
    c.run("pip install -e '.[dev,tests]'")


@task
def setup_java(c: Context):
    c.run("conda install -y -c 'bioconda/label/cf201901' java-jdk")


@task
def setup(c: Context):
    git_init(c)
    setup_java(c)
    install(c)


@task
def update(c: Context):
    echo_header(f"{Emo.DO} Updating project")
    c.run("pip install --upgrade -e '.[dev,tests]'")


@task
def test(c: Context):
    echo_header(f"{Emo.TEST} Running tests")
    test_result: Result = c.run(
        "pytest -n auto -rfE --failed-first -p no:typeguard -p no:cov --disable-warnings -q",
        warn=True,
        pty=True,
    )

    # If "failed" in the pytest results
    if "failed" in test_result.stdout:
        print("\n\n\n")
        echo_header("Failed tests")

        # Get lines with "FAILED" in them from the .pytest_results file
        failed_tests = [
            line
            for line in Path("tests/.pytest_results").read_text().splitlines()
            if line.startswith("FAILED")
        ]

        for line in failed_tests:
            # Remove from start of line until /test_
            line_sans_prefix = line[line.find("test_") :]

            # Keep only that after ::
            line_sans_suffix = line_sans_prefix[line_sans_prefix.find("::") + 2 :]
            print(f"FAILED {Emo.FAIL} #{line_sans_suffix}     ")

        exit(0)


@task
def lint(c: Context):
    pre_commit(c)


@task
def pr(c: Context):
    add_and_commit(c)
    lint(c)
    test(c)
    update_branch(c)
    update_pr(c)
