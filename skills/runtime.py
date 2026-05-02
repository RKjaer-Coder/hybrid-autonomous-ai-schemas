from __future__ import annotations

from kernel import runtime_compat as _runtime_compat

globals().update(
    {
        name: getattr(_runtime_compat, name)
        for name in dir(_runtime_compat)
        if not name.startswith("__")
    }
)


def main() -> int:
    patched: dict[str, object] = {}
    for name, value in globals().items():
        if name.startswith("__") or name in {"_runtime_compat", "main", "patched"}:
            continue
        if hasattr(_runtime_compat, name):
            patched[name] = getattr(_runtime_compat, name)
            setattr(_runtime_compat, name, value)
    try:
        return _runtime_compat.main()
    finally:
        for name, value in patched.items():
            setattr(_runtime_compat, name, value)


if __name__ == "__main__":
    raise SystemExit(main())
