# GraphBLAS-sharp

GraphBLAS# is a GPGPU-based [GraphBLAS](https://graphblas.org/) implementation in F#. To utilize GPGPUs we use [Brahma.FSharp](https://github.com/YaccConstructor/Brahma.FSharp). So, GraphBLAS# can utilize any OpenCL compatible device.

### Features
- ```Option<'t>``` to solve [explicit/implicit zeroes problem](https://github.com/GraphBLAS/LAGraph/issues/28#issuecomment-542952115). If graph has labels of type ```'t``` then adjacency matrix is ```Matrix<Option<'t>>```. Sparse storage contains only values for ```Some<'t>``` cells. 
- Elementwise operations have type ```AtLeastOne<'t1,'t2> -> Option<'t3>``` to be shure that ```None op None``` is ```None```. Also developer explicitly controls what should be ```None```. ```AtLeastOne``` defined as fallows:
  ```fsharp
  type AtLeastOne<'t1, 't2> =
     | Both of 't1*'t2
     | Left of 't1
     | Right of 't2
  ```
  So, type of matrix-matrix elementwise oertion is ```Matrix<Option<'t1>> -> Matrix<Option<'t2>> -> (AtLeastOne<'t1,'t2> -> Option<'t3>) -> Matrix<Option<'t3>>```. 
- No semirings. Just functions. Ofcourse one can implement semirings on the top of provided API.

### Operations
#### Matrix-Matrix
- [x] COO-COO element-wize
- [x] CSR-CSR element-wize
- [ ] CSR-CSR multiplication
- [ ] COO transpose
- [ ] CSR transpose
#### Vector-Matrix
- [x] Dense-CSR multiplication
- [ ] COO-CSR multiplication
#### Vector-Vector
- [x] Dense-Dense element-wise
- [ ] ...

### Evaluation
Matrices from [SuiteSparse matrix collection]() which we choose for evaluation.
<table>
<thead>
  <tr>
    <th>Matrix</th>
    <th>Size</th>
    <th>NNZ</th>
    <th>Squared matrix NNZ</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>wing</td>
    <td>62 032</td>
    <td>243 088</td>
    <td>714 200</td>
  </tr>
  <tr>
    <td>luxembourg osm</td>
    <td>114 599</td>
    <td>119 666</td>
    <td>393 261</td>
  </tr>
  <tr>
    <td>amazon0312</td>
    <td>400 727</td>
    <td>3 200 440</td>
    <td>14 390 544</td>
  </tr>
  <tr>
    <td>amazon-2008</td>
    <td>735 323</td>
    <td>5 158 388</td>
    <td>25 366 745</td>
  </tr>
  <tr>
    <td>web-Google</td>
    <td>916 428</td>
    <td>5 105 039</td>
    <td>29 710 164</td>
  </tr>
  <tr>
    <td>webbase-1M</td>
    <td>1 000 005</td>
    <td>3 105 536</td>
    <td>51 111 996</td>
  </tr>
  <tr>
    <td>cit-Patents</td>
    <td>3 774 768</td>
    <td>16 518 948</td>
    <td>469</td>
  </tr>
</tbody>
</table>
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

## Build
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
 

