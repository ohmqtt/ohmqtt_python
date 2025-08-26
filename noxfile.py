import contextlib
import nox
import sqlite3
import sys

nox.options.default_venv_backend = "uv|virtualenv"

PYPROJECT = nox.project.load_toml("pyproject.toml")

ALL_PYTHONS = [
    cls.split()[-1]
    for cls in PYPROJECT["project"]["classifiers"]
    if cls.startswith("Programming Language :: Python :: 3.")
]


@nox.session(python=ALL_PYTHONS)
def tests(session: nox.Session) -> None:
    coverage_file = f".coverage.{sys.platform}.{session.python}"
    pytest_env = {
        "COVERAGE_FILE": coverage_file,
    }

    session.install(".")
    session.install("--group", "dev")
    session.run("ruff", "check")
    session.run("typos")
    session.run("mypy")
    session.run("complexipy", "-d", "low", "ohmqtt", "examples", "tests")
    session.run("pytest", env=pytest_env)

    if sys.platform.startswith("win"):
        with contextlib.closing(sqlite3.connect(coverage_file)) as con, con:
            con.execute("UPDATE file SET path = REPLACE(path, '\\', '/')")
            con.execute("DELETE FROM file WHERE SUBSTR(path, 2, 1) == ':'")


@nox.session(default=False)
def cover(session: nox.Session) -> None:
    """Coverage analysis."""
    session.install("coverage[toml]>=7.3")
    session.run("coverage", "combine")
    session.run("coverage", "report", "--fail-under=100", "--show-missing")
    session.run("coverage", "erase")
