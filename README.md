# GraphBLAS-sharp

[![FAKE Build](https://github.com/YaccConstructor/GraphBLAS-sharp/actions/workflows/build-on-push.yml/badge.svg)](https://github.com/YaccConstructor/GraphBLAS-sharp/actions/workflows/build-on-push.yml) 
[![NuGet Badge](https://buildstats.info/nuget/GraphBLAS-sharp)](https://www.nuget.org/packages/GraphBLAS-sharp/)
[![License](https://img.shields.io/badge/License-MIT-red.svg)](https://opensource.org/licenses/MIT)





GraphBLAS# is a GPGPU-based [GraphBLAS](https://graphblas.org/)-like API implementation in F#. To utilize GPGPUs we use [Brahma.FSharp](https://github.com/YaccConstructor/Brahma.FSharp). So, GraphBLAS# can utilize any OpenCL-compatible device.

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
- Minimal core: high-order functions allows us to minimaze core by functions unification. For example, such functions as matrix-matrix addition, matrix-matrix element-wise multiplication, masking all are partial case of `map2` function.

### Operations
- **Matrix-Matrix**
  - [x] COO-COO `map2`
  - [x] CSR-CSR `map2`
  - [x] CSR-CSR multiplication
- **Vector-Matrix**
  - [x] Dense-CSR multiplication
  - [ ] COO-CSR multiplication
- **Vector-Vector**
  - [x] Dense-Dense element-wise
  - [x] Sparse-Sparse element-wise
  - [ ] ...
- **Matrix**
  - [x] `map`
  - [x] COO transpose
  - [x] CSR transpose
  - [ ] `iter`
  - [ ] ...
- **Vector**
  - [x] `map`
  - [ ] `iter`
  - [ ] `filter`
  - [ ] `contains`
  - [ ] ...  

### Graph Analysis Algorithms
- [ ] BFS
- [ ] Parent BFS
- [ ] Single Source Shortest Path
- [ ] Triangles Counting
- [ ] Local Clustering Coefficient
- [ ] Community Detection using Label Propagation
- [ ] Weakly Connected Components
- [ ] PageRank

### Evaluation
Matrices from [SuiteSparse matrix collection](https://sparse.tamu.edu/) which we choose for evaluation.
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

Element-wise matrix-matrix evaluation results presented below. Time is measured in milliseconds. We perform our experiments on the PC with Ubuntu 20.04 installed and with the following hardware configuration: Intel Core i7–4790 CPU, 3.60GHz, 32GB DDR4 RAM with GeForce GTX 2070, 8GB GDDR6, 1.41GHz. 

<table>
<thead>
  <tr>
    <th rowspan="3">Matrix</th>
    <th colspan="4">Elemint-wise addition</th>
    <th colspan="2">Elemint-wise multiplication</th>
  </tr>
  <tr>
    <th colspan="2">GraphBLAS-sharp</th>
    <th rowspan="2">SuiteSparse</th>
    <th rowspan="2">CUSP</th>
    <th rowspan="2">GraphBLAS-sharp</th>
    <th rowspan="2">SuiteSparse</th>
  </tr>
  <tr>
    <th>No AtLeastOne</th>
    <th>AtLeastOne</th>
  </tr>
</thead>
<tbody>
  <tr>
    <td>wing</td>
    <td>4,3 ± 0,8</td>
    <td>4,3 ± 0,6</td>
    <td>2,7 ± 0,9</td>
    <td>1,5 ± 0,0</td>
    <td>3,7 ± 0,5</td>
    <td>3,5 ± 0,4</td>
  </tr>
  <tr>
    <td>luxembourg osm</td>
    <td>4,9 ± 0,7</td>
    <td>4,1 ± 0,5</td>
    <td>3,0 ± 1,1</td>
    <td>1,2 ± 0,1</td>
    <td>3,8 ± 0,6</td>
    <td>3,0 ± 0,6</td>
  </tr>
  <tr>
    <td>amazon0312</td>
    <td>22,3 ± 1,3</td>
    <td>22,1 ± 1,3</td>
    <td>33,4 ± 0,8</td>
    <td>11,0 ± 1,4</td>
    <td>18,7 ± 0,9</td>
    <td>35,7 ± 1,4</td>
  </tr>
  <tr>
    <td>amazon-2008</td>
    <td>38,7 ± 0,8</td>
    <td>39,0 ± 1,0</td>
    <td>55,9 ± 1,0</td>
    <td>19,1 ± 1,4</td>
    <td>34,5 ± 1,0</td>
    <td>58,9 ± 1,9</td>
  </tr>
  <tr>
    <td>web-Google</td>
    <td>43,4 ± 0,8</td>
    <td>43,4 ± 1,1</td>
    <td>67,2 ± 7,5</td>
    <td>21,3 ± 1,3</td>
    <td>39,0 ± 1,2</td>
    <td>66,2 ± 0,4</td>
  </tr>
  <tr>
    <td>webbase-1M</td>
    <td>63,6 ± 1,1</td>
    <td>63,7 ± 1,3</td>
    <td>86,5 ± 2,0</td>
    <td>24,3 ± 1,3</td>
    <td>54,5 ± 0,7</td>
    <td>37,6 ± 5,6</td>
  </tr>
  <tr>
    <td>cit-Patents</td>
    <td>26,9 ± 0,7</td>
    <td>26,0 ± 0,7</td>
    <td>183,4 ± 5,4</td>
    <td>10,8 ± 0,6</td>
    <td>24,3 ± 0,7</td>
    <td>162,2 ± 1,7</td>
  </tr>
</tbody>
</table>


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
 

