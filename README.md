# C++ VS Code Template (Linux, vcpkg)

Minimal starter project for C++ with VS Code + CMake + vcpkg manifest mode.

## Requirements
- Linux with `gcc`/`g++`, `cmake`, `gdb`
- [vcpkg](https://github.com/microsoft/vcpkg) installed
- VS Code with C++ and CMake Tools extensions

## Quick start
```bash
# Configure and build
cmake --preset=debug
cmake --build --preset=debug

# Run
./build/bin/hello
```

Output should be a JSON message:
```json
{
    "msg": "Hello, World! 👋 from C++ with vcpkg (nlohmann-json)"
}
```

## Notes
- Dependencies are declared in `vcpkg.json`.
- vcpkg will fetch `nlohmann-json` automatically.
