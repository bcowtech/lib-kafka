**May 2020**  |  **v0.1**

# Go Kafka instruction (Windows Version)

**Requirements**:
  - **Strawberry Perl** for Windows v5.30.1+ 
  - **CMake** for Windows v3.19.1+
  - **librdkafka** 1.5.2


### Install Perl
  1. Download `strawberry-perl-5.30.xxx-64bit.msi` from https://www.perl.org/get.html
  2. Install perl via executing install package.
  3. Remember to set Perl to system environment
     - Check `C:\Strawberry\c\bin` in your system environment %PATH%
     - Open a command type `gcc -v`, you will get
       ```bash
       Using built-in specs.
       COLLECT_GCC=C:\Strawberry\c\bin\gcc.exe
       COLLECT_LTO_WRAPPER=C:/Strawberry/c/bin/../libexec/gcc/x86_64-w64-mingw32/8.3.0/lto-wrapper.exe
       Target: x86_64-w64-mingw32
       ....
       ```

      - Ensure your COLLECT_LTO_WRAPPER contains the location `C:\Strawberry\c`
---
### Install CMake
  1. Download `Windows win64-x64 Installer` from https://cmake.org/download/
  2. Install CMake via executing install package.
---
### Download librdkafka 1.5.2
  1. Download ` librdkafka 1.5.2` from https://github.com/edenhill/librdkafka/archive/v1.5.2.zip
  2. Enter the librdkafka folder
      ```
      $ ./packaging/mingw-w64/configure-build-msys2-mingw.sh .
      ```
  3. Copy files to Strawberry/c/
      - Copy `librdkafka/dest/bin/*.* `to `Strawberry/c/bin` 
      - Copy `librdkafka/dest/include/*.*` to `Strawberry/c/include` 
      - Copy `librdkafka/dest/lib/*.dll.a` to `Strawberry/c/lib` 
      - Copy `librdkafka/dest/lib/pkgconfig/*.*` to `Strawberry/c/lib/pkgconfig` 
   rdkafka.pc & rdkafka++.pc
  4. modified  `rdkafka.pc` and `rdkafka++.pc` prefix to /Strawberry/c
      ```
      prefix=C:/Strawberry/c         <-- change this
      exec_prefix=${prefix}
      includedir=${prefix}/include
      libdir=${prefix}/lib
      ....
      ```

---
### Check your librdkafka
1. Create a new go project
2. Open a command type
   ```
   go mod init ...
   go get -u gopkg.in/confluentinc/confluent-kafka-go.v1/kafka@1.5.2
   ```

3. `<GOPATH>` /pkg/mod/gopkg.in/confluentinc/confluent-kafka-go.v1@v1.5.2/kafka/00version.go
   - add  `#cgo LDFLAGS: -lrdkafka` before `#include <librdkafka/rdkafka.h>`
4. `<GOPATH>`/pkg/mod/gopkg.in/confluentinc/confluent-kafka-go.v1@v1.5.2/kafka/librdkafka/rdkafka.h
   - Find/replace `_MSC_VER` with `__MINGW64__` twice
