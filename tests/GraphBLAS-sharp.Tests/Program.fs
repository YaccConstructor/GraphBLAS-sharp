open Expecto
open GraphBLAS.FSharp.Tests.Backend

let matrixTests =
    testList
        "Matrix tests"
        [ Matrix.Convert.tests
          Matrix.Map2.addTests
          Matrix.Map2.addAtLeastOneTests
          Matrix.Map2.mulAtLeastOneTests
          Matrix.Map2.addAtLeastOneToCOOTests
          Matrix.Mxm.tests
          Matrix.Transpose.tests ]
    |> testSequenced

let commonTests =
    let clArrayTests =
        testList
            "ClArray"
            [ Common.ClArray.PrefixSum.tests
              Common.ClArray.RemoveDuplicates.tests
              Common.ClArray.Copy.tests
              Common.ClArray.Replicate.tests
              Common.ClArray.Exists.tests
              Common.ClArray.Map.tests
              Common.ClArray.Map2.addTests
              Common.ClArray.Map2.mulTests
              Common.ClArray.Choose.tests ]

    testList
        "Common tests"
        [ clArrayTests
          Common.BitonicSort.tests
          Common.Scatter.tests
          Common.Reduce.tests
          Common.Sum.tests ]
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
          Vector.AssignByMask.tests
          Vector.AssignByMask.complementedTests
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
