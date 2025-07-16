import nox
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
    complexipy_env = {"PYTHONUTF8": "1"} if sys.platform.startswith("win") else None
    session.install(".")
    session.install("--group", "dev")
    session.run("ruff", "check")
    session.run("typos")
    session.run("mypy")
    session.run("complexipy", "-d", "low", "ohmqtt", "examples", "tests", env=complexipy_env)
    session.run("pytest")
