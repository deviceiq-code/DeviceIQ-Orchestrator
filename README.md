# DeviceIQ Orchestrator

Orchestrator is a tool to manage DeviceIQ devices remotely.

## Requirements
- Linux with `gcc`/`g++`, `cmake`, `gdb`
- [vcpkg](https://github.com/microsoft/vcpkg) installed
- VS Code with C++ and CMake Tools extensions

## Quick start
```bash
# Configure and build
cmake --preset=debug
cmake --build --preset=debug

## Notes
- Dependencies are declared in `vcpkg.json`.
- vcpkg will fetch `nlohmann-json` automatically.
