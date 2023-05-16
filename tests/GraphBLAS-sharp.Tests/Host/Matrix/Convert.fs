module GraphBLAS.FSharp.Tests.Host.Matrix.Convert

open Expecto
open GraphBLAS.FSharp.Objects
open GraphBLAS.FSharp.Tests

let makeTest isEqual zero (array: 'a [,]) =
    let cooMatrix =
        Matrix.COO.FromArray2D(array, isEqual zero)

    let actual = cooMatrix.ToCSR

    let expected =
        Matrix.CSR.FromArray2D(array, isEqual zero)

    Utils.compareCSRMatrix isEqual actual expected

let createTest<'a when 'a: struct> isEqual (zero: 'a) =
    makeTest isEqual zero
    |> testPropertyWithConfig Utils.defaultConfig $"%A{typeof<'a>}"

let tests =
    [ createTest (=) 0
      createTest (=) false ]
    |> testList "Convert"
