# CODEx Guide - JAAQL-monitor

## What This Repo Is
Python-based JAAQL monitor/CLI utility that appears to package into a Windows executable and can also run as Python.

## Key Files
- `README.md` - usage, input format, and build notes
- `requirements.txt` - runtime dependencies
- `setup.py` - package metadata/build config
- `build.bat`, `build_test.bat` - Windows build scripts
- `update.py` - update/build helper logic
- `monitor/` - application code

## Usage Notes (from README)
- CLI is invoked with a credentials file path.
- Tool accepts commands over stdin and supports separators like `\\p` and `\\g`.
- Errors may be written to stdout/stderr (README wording mentions both; verify in code when debugging).

## Build / Runtime Notes
- README references Python 3.8 for building the executable.
- README also mentions local running with Python 3.11.
- Confirm target Python version before changing packaging or syntax.

## Working Rules
- If changing CLI behavior, preserve separator semantics (`\\p`, `\\g`) unless explicitly asked.
- Validate stdin parsing carefully; this kind of tool is easy to regress silently.
- If packaging changes are needed, inspect `build.bat` and `setup.py` together.

## First Read For Any Task
1. `README.md`
2. `monitor/` source files
3. `build.bat` / `build_test.bat` if task involves packaging