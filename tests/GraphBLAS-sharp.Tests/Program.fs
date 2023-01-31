open Expecto
open GraphBLAS.FSharp.Tests.Backend

let matrixTests =
    testList
        "Matrix tests"
        [ Matrix.Convert.tests
          Matrix.Elementwise.elementwiseAddTests
          Matrix.Elementwise.elementwiseAddAtLeastOneTests
          Matrix.Elementwise.elementwiseMulAtLeastOneTests
          Matrix.Elementwise.elementwiseAddAtLeastOneToCOOTests
          Matrix.Mxm.tests
          Matrix.Transpose.tests ]
    |> testSequenced

let commonTests =
    testList
        "Common tests"
        [ Common.BitonicSort.tests
          Common.PrefixSum.tests
          Common.Scatter.tests
          Common.RemoveDuplicates.tests
          Common.Copy.tests
          Common.Replicate.tests
          Common.Reduce.tests
          Common.Sum.tests
          Common.Exists.tests ]
    |> testSequenced

let vectorTests =
    testList
        "Vector tests"
        [ Vector.SpMV.tests
          Vector.ZeroCreate.tests
          Vector.OfList.tests
          Vector.Copy.tests
          Vector.Convert.tests
          Vector.Map2.addTests
          Vector.Map2.mulTests
          Vector.Map2.addAtLeastOneTests
          Vector.Map2.mulAtLeastOneTests
          Vector.Map2.addGeneralTests
          Vector.Map2.mulGeneralTests
          Vector.Map2.complementedGeneralTests
          Vector.FillSubVector.tests
          Vector.FillSubVector.complementedTests
          Vector.Reduce.tests ]
    |> testSequenced

let algorithmsTests =
    testList "Algorithms tests" [ Algorithms.BFS.tests ]
    |> testSequenced

[<Tests>]
let allTests =
    testList
        "All tests"
        [ commonTests
          matrixTests
          vectorTests
          algorithmsTests ]
    |> testSequenced

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [] argv
