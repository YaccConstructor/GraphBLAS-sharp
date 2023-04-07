open Expecto
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests

let matrixTests =
    testList
        "Matrix tests"
        [ Matrix.Convert.tests
          Matrix.Map2.addTests
          Matrix.Map2.addAtLeastOneTests
          Matrix.Map2.mulAtLeastOneTests
          Matrix.Map2.addAtLeastOneToCOOTests
          Matrix.Map.notTests
          Matrix.Map.addTests
          Matrix.Map.mulTests
          Matrix.Transpose.tests

          Matrix.SpGeMM.Masked.tests
          Matrix.SpGeMM.Expand.generalTests ]
    |> testSequenced

let commonTests =
    let reduceTests =
        testList
            "Reduce"
            [ Common.Reduce.ByKey.sequentialTest
              Common.Reduce.ByKey.sequentialSegmentTests
              Common.Reduce.ByKey.oneWorkGroupTest
              Common.Reduce.ByKey.testsByKey2DSegmentsSequentialOption
              Common.Reduce.Reduce.tests
              Common.Reduce.Sum.tests ]

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
              Common.ClArray.Choose.allTests ]

    let sortTests =
        testList
            "Sort"
            [ Common.Sort.Bitonic.tests
              Common.Sort.Radix.testByKeys
              Common.Sort.Radix.testKeysOnly ]

    testList
        "Common tests"
        [ clArrayTests
          sortTests
          reduceTests
          Common.Scatter.allTests
          Common.Gather.allTests ]
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
        [ matrixTests
          vectorTests
          commonTests
          algorithmsTests ]
    |> testSequenced

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [] argv
