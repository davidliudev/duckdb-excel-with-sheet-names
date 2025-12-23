# Repository Guidelines

## Project Structure

- `src/excel/`: DuckDB Excel extension sources.
  - `src/excel/excel_extension.cpp`: extension entry points and registration.
  - `src/excel/xlsx/`: XLSX read/write implementation (zip + XML parsing).
  - `src/excel/numformat/`: number-formatting implementation used by `text(...)`.
- `test/sql/excel/`: DuckDB SQL logic tests (`*.test`).
- `test/data/xlsx/`: XLSX fixtures used by tests.
- `duckdb/`, `extension-ci-tools/`: git submodules required for builds/CI.
- `vcpkg.json`, `vcpkg_ports/`: pinned native dependencies (e.g., `expat`, `minizip-ng`, `zlib`).

## Setup, Build, and Development Commands

1) Initialize submodules (required):

`git submodule update --init --recursive`

2) Build/test via the DuckDB extension tooling (provided by `extension-ci-tools`):

- `make`: build the extension using the shared extension makefiles.
- `make test`: run the DuckDB test runner for `test/sql/**` (including `require excel` tests).
- `make clean`: remove build artifacts.

If targets differ in your environment, run `make -n` or inspect the included makefile in `extension-ci-tools`.

## Coding Style & Naming

- C/C++ formatting: follow `.clang-format` (tabs for indentation, width 4, 120 col limit). Prefer running `clang-format` on touched files only.
- Static analysis: `.clang-tidy` treats warnings as errors; keep changes warning-free.
- Naming (from lint config): `CamelCase` for classes/functions; `lower_case` for variables/members/parameters; `UPPER_CASE` for macros.

## Testing Guidelines

- Add coverage by extending `test/sql/excel/*.test` and, when needed, placing minimal XLSX fixtures in `test/data/xlsx/`.
- Keep fixtures small and deterministic; avoid adding large binaries unless necessary.

## Commit & Pull Request Guidelines

- Commit messages in history are short, imperative summaries (e.g., `bump duckdb`, `fix test`, `Unescape sheet names`).
- PRs should explain user-visible behavior changes, include new/updated tests for bug fixes, and link relevant issues/PRs when applicable.

## Security & Configuration Tips

- Treat XLSX input as untrusted: avoid adding tests that depend on external/networked resources and keep parsing changes defensive.
