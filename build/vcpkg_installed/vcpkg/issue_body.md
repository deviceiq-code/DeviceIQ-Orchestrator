Package: nlohmann-json:x64-linux@3.12.0#1

**Host Environment**

- Host: x64-linux
- Compiler: GNU 13.3.0
-    vcpkg-tool version: 2025-07-21-d4b65a2b83ae6c3526acd1c6f3b51aff2a884533
    vcpkg-scripts version: b1e15efef6 2025-09-06 (14 hours ago)

**To Reproduce**

`vcpkg install `

**Failure logs**

```
Downloading https://github.com/nlohmann/json/archive/v3.12.0.tar.gz -> nlohmann-json-v3.12.0.tar.gz
Successfully downloaded nlohmann-json-v3.12.0.tar.gz
-- Extracting source /home/fernando/vcpkg/downloads/nlohmann-json-v3.12.0.tar.gz
-- Applying patch fix-4742_std_optional.patch
-- Using source at /home/fernando/vcpkg/buildtrees/nlohmann-json/src/v3.12.0-087f34fe5c.clean
-- Configuring x64-linux
-- Building x64-linux-dbg
-- Building x64-linux-rel
-- Fixing pkgconfig file: /home/fernando/vcpkg/packages/nlohmann-json_x64-linux/share/pkgconfig/nlohmann_json.pc
CMake Error at scripts/cmake/vcpkg_find_acquire_program.cmake:166 (message):
  Could not find pkg-config.  Please install it via your package manager:

      sudo apt-get install pkg-config
Call Stack (most recent call first):
  scripts/cmake/vcpkg_fixup_pkgconfig.cmake:193 (vcpkg_find_acquire_program)
  ports/nlohmann-json/portfile.cmake:30 (vcpkg_fixup_pkgconfig)
  scripts/ports.cmake:206 (include)



```

**Additional context**

<details><summary>vcpkg.json</summary>

```
{
  "name": "hello-cpp",
  "version-string": "0.1.0",
  "dependencies": [
    "nlohmann-json"
  ]
}

```
</details>
