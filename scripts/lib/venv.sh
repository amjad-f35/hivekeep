# shellcheck shell=bash
# Python virtual environment helpers for scripts that run Python tooling.
# The tooling needs Python 3.10 or newer.

# hk_python prints the interpreter new venvs are built from: HK_PYTHON when
# set, else the first python3.N (newest first) or python3 on PATH that is
# 3.10+. Fails when there is none.
hk_python() {
  local c
  for c in ${HK_PYTHON:-} python3.14 python3.13 python3.12 python3.11 python3.10 python3; do
    if command -v "${c}" >/dev/null 2>&1 \
      && "${c}" -c 'import sys; sys.exit(sys.version_info < (3, 10))' >/dev/null 2>&1; then
      command -v "${c}"
      return 0
    fi
    if [[ -n "${HK_PYTHON:-}" && "${c}" == "${HK_PYTHON}" ]]; then
      echo "error: HK_PYTHON=${HK_PYTHON} is not a Python 3.10+ interpreter" >&2
      return 1
    fi
  done
  echo "error: Python 3.10 or newer is required (found $(python3 --version 2>&1 || echo none)); set HK_PYTHON" >&2
  return 1
}

# hk_venv_usable reports whether the venv at $1 can actually run: an
# interrupted create leaves no activate script, an interpreter upgrade leaves
# a dangling python3, and a venv built from an older interpreter is too old.
hk_venv_usable() {
  [[ -f "$1/bin/activate" && -x "$1/bin/python3" ]] \
    && "$1/bin/python3" -c 'import sys; sys.exit(sys.version_info < (3, 10))' >/dev/null 2>&1
}

# hk_venv_ensure creates (or rebuilds) the venv at $1 when it is not usable.
hk_venv_ensure() {
  if hk_venv_usable "$1"; then
    return 0
  fi
  local py
  py="$(hk_python)" || return 1
  echo "Creating virtual environment in $1 ($("${py}" --version))..."
  "${py}" -m venv --clear "$1"
  "$1/bin/pip" install --quiet --upgrade pip
}

# hk_venv_install installs requirements file $2 into the venv at $1.
hk_venv_install() {
  "$1/bin/pip" install --quiet -r "$2"
}
