import nox
import sys

nox.options.default_venv_backend = "uv|virtualenv"
py_versions = ("3.10", "3.11", "3.12", "3.13")


@nox.session(python=py_versions)
def tests(session):
    complexipy_env = {"PYTHONUTF8": "1"} if sys.platform.startswith("win") else None
    session.install(".")
    session.install("--group", "dev")
    session.run("ruff", "check")
    session.run("typos")
    session.run("mypy")
    session.run("complexipy", "-d", "low", "ohmqtt", "examples", "tests", env=complexipy_env)
    session.run("pytest", "--cov-report=term-missing")
