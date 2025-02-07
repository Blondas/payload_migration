-use properly .toml file with poetry, 
    add to toml file: [tool.poetry.dev-dependencies] pytest-stubs = "*"

- pydantic - zamiast case class
- podwojny podkreslnik to private

4. Jakie biblioteki
  - pydantic
  - request
  - fastapi
  - loguru
  - poetry
  - do bindowania projektu: poetry + toml

5. Jaki setup projektu, testow: `pyproject.toml`? czy inny?

8. Jak zrobiony config projektu, gdzie trzymane pliki
- pydantic ma modul do konfigurowania projektu pydantic-settings, dot-env ladujacy z pliku srodowiskowego

10. Jakiego lintera i innych tooli uzywa do devovania
to wszytko odpalic sobie z shela ze skryptu
mypy - tak se dziala
black - tez dziala mniej wiecej
isort - import sort
bandit - bledy bezpieczenstwa
ruff - kolejny linter