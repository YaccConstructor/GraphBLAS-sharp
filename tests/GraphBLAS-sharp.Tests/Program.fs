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
          Matrix.SpGeMM.Expand.generalTests
          Matrix.SpGeMM.Masked.tests
          Matrix.Transpose.tests
          Matrix.RowsLengths.tests ]
    |> testSequenced

let commonTests =
    let scanTests =
        testList
            "Scan"
            [ Common.Scan.ByKey.sequentialSegmentsTests
              Common.Scan.PrefixSum.tests ]

    let reduceTests =
        testList
            "Reduce"
            [ Common.Reduce.ByKey.allTests
              Common.Reduce.Reduce.tests
              Common.Reduce.Sum.tests ]

    let clArrayTests =
        testList
            "ClArray"
            [ Common.ClArray.RemoveDuplicates.tests
              Common.ClArray.Copy.tests
              Common.ClArray.Replicate.tests
              Common.ClArray.Exists.tests
              Common.ClArray.Map.tests
              Common.ClArray.Map2.addTests
              Common.ClArray.Map2.mulTests
              Common.ClArray.Choose.allTests
              Common.ClArray.ChunkBySize.allTests
              Common.ClArray.Blit.tests
              Common.ClArray.Concat.tests
              Common.ClArray.Fill.tests
              Common.ClArray.Pairwise.tests ]

    let sortTests =
        testList
            "Sort"
            [ Common.Sort.Bitonic.tests
              Common.Sort.Radix.allTests ]

    testList
        "Common tests"
        [ clArrayTests
          sortTests
          reduceTests
          scanTests
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
          commonTests
          vectorTests
          algorithmsTests ]
    |> testSequenced

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [] argv
