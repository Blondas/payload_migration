-use properly .toml file with poetry, add to toml file: [tool.poetry.dev-dependencies] pytest-stubs = "*"

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

6. Jak robic deployment - sie pakuje w jakas paczke, czy deployuje testy, jakie sa dobre praktyki

7. Ustawienie loggera

8. Jak zrobiony config projektu, gdzie trzymane pliki
- pydantic ma modul do konfigurowania projektu pydantic-settings, dot-env ladujacy z pliku srodowiskowego

9. Co wyeksponowane w `__init__.py` - linterfejs i implementacja, import z `__init__.py` czy bezposrednio z plikow?

10. Jakiego lintera i innych tooli uzywa do devovania
to wszytko odpalic sobie z shela ze skryptu
mypy - tak se dziala
black - tez dziala mniej wiecej
isort - import sort
bandit - bledy bezpieczenstwa
ruff - kolejny linter

11. Czy mial taki przypadek jak ja z db ktore nie chodzi na osx?
12. Jak mockowac? czy uzywa fixtures?