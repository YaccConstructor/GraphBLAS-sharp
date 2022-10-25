# GraphBLAS-sharp

GraphBLAS# is a GPGPU-based GraphBLAS implementation in F#. To utilize GPGPUs we use [Brahma.FSharp](https://github.com/YaccConstructor/Brahma.FSharp). So, GraphBLAS# can utilize any OpenCL compatible device.

---

## Builds

GitHub Actions |
:---: |
[![GitHub Actions](https://github.com/YaccConstructor/GraphBLAS-sharp/workflows/Build%20master/badge.svg)](https://github.com/YaccConstructor/GraphBLAS-sharp/actions?query=branch%3Amaster) |
[![Build History](https://buildstats.info/github/chart/YaccConstructor/GraphBLAS-sharp)](https://github.com/YaccConstructor/GraphBLAS-sharp/actions?query=branch%3Amaster) |

## NuGet 

Package | Stable | Prerelease
--- | --- | ---
GraphBLAS-sharp | [![NuGet Badge](https://buildstats.info/nuget/GraphBLAS-sharp)](https://www.nuget.org/packages/GraphBLAS-sharp/) | [![NuGet Badge](https://buildstats.info/nuget/GraphBLAS-sharp?includePreReleases=true)](https://www.nuget.org/packages/GraphBLAS-sharp/)

## Contributing
Contributions, issues and feature requests are welcome.
Feel free to check [issues](https://github.com/YaccConstructor/GraphBLAS-sharp/issues) page if you want to contribute.


### Build
Make sure the following **requirements** are installed on your system:
- [dotnet SDK](https://dotnet.microsoft.com/en-us/download/dotnet/5.0) 5.0 or higher
- OpenCL-compatible device and respective OpenCL driver

To build and run all tests:

- on Windows
```cmd
build.cmd 
```

- on Linux/macOS
```shell
./build.sh 
```
To find more options look at [MiniScaffold](https://github.com/TheAngryByrd/MiniScaffold). We use it in our project.

## License
This project licensed under MIT License. License text can be found in the [license file](https://github.com/YaccConstructor/GraphBLAS-sharp/blob/master/LICENSE.md).
 

