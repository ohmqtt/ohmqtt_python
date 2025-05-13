import nox
import sys

nox.options.default_venv_backend = "uv|virtualenv"
py_versions = ("3.10", "3.11", "3.12", "3.13")


@nox.session(python=py_versions)
def tests(session):
    session.install(".")
    session.install("--group", "dev")
    session.run("ruff", "check")
    session.run("typos")
    session.run("mypy")
    if sys.platform != "win32":
        session.run("complexipy", "-d", "low", "ohmqtt", "examples", "tests")
    else:
        session.log("Skipping complexipy check on Windows: https://github.com/rohaquinlop/complexipy/issues/67")
    session.run("pytest", "--cov-report=term-missing")
