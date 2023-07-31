open Expecto
open GraphBLAS.FSharp.Tests.Backend
open GraphBLAS.FSharp.Tests

let matrixTests =
    testList
        "Matrix"
        [ Matrix.Convert.tests
          Matrix.Map2.allTests
          Matrix.Map.allTests
          Matrix.Merge.allTests
          Matrix.Transpose.tests
          Matrix.RowsLengths.tests
          Matrix.ByRows.tests
          Matrix.ExpandRows.tests
          Matrix.SubRows.tests
          Matrix.Kronecker.tests

          Matrix.SpGeMM.Expand.tests
          Matrix.SpGeMM.Masked.tests ]
    |> testSequenced

let commonTests =
    let scanTests =
        testList
            "Scan"
            [ Common.Scan.ByKey.tests
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
              Common.ClArray.Pairwise.tests
              Common.ClArray.UpperBound.tests
              Common.ClArray.Set.tests
              Common.ClArray.Item.tests ]

    let sortTests =
        testList
            "Sort"
            [ Common.Sort.Bitonic.tests
              Common.Sort.Radix.allTests ]

    testList
        "Common"
        [ Common.Scatter.allTests
          Common.Gather.allTests
          Common.Merge.tests
          clArrayTests
          sortTests
          reduceTests
          scanTests ]
    |> testSequenced

let vectorTests =
    testList
        "Vector"
        [ Vector.SpMV.tests
          Vector.ZeroCreate.tests
          Vector.OfList.tests
          Vector.Copy.tests
          Vector.Convert.tests
          Vector.Map2.allTests
          Vector.AssignByMask.tests
          Vector.AssignByMask.complementedTests
          Vector.Reduce.tests
          Vector.Merge.tests ]
    |> testSequenced

let algorithmsTests =
    testList "Algorithms tests" [ Algorithms.BFS.tests ]
    |> testSequenced

let deviceTests =
    testList
        "Device"
        [ matrixTests
          commonTests
          vectorTests
          algorithmsTests ]
    |> testSequenced

let hostTests =
    testList
        "Host"
        [ Host.Matrix.FromArray2D.tests
          Host.Matrix.Convert.tests
          Host.IO.MtxReader.test ]
    |> testSequenced

[<Tests>]
let allTests =
    testList "All" [ deviceTests; hostTests ]
    |> testSequenced

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [] argv
