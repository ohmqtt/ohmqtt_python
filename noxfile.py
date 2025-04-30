import nox

nox.options.default_venv_backend = "uv|virtualenv"
py_versions = ("3.10", "3.11", "3.12", "3.13")


@nox.session(python=py_versions)
def tests(session):
    session.install(".")
    session.install(
        "complexipy",
        "cryptography",
        "mypy",
        "pytest",
        "pytest-cov",
        "pytest-mock",
        "pyyaml",
        "ruff",
        "typos",
    )
    session.run("ruff", "check")
    session.run("typos")
    session.run("mypy")
    session.run("complexipy", "-d", "low", "ohmqtt", "examples")
    session.run("pytest", "--cov-report=term-missing")

