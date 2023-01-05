open Expecto
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Tests.

[<Tests>]
let matrixTests =
    testList
        "Matrix tests"
        [ Backend.Matrix.Convert.tests
          Backend.Matrix.Elementwise.elementwiseAddTests
          Backend.Matrix.Elementwise.elementwiseAddAtLeastOneTests
          Backend.Matrix.Elementwise.elementwiseMulAtLeastOneTests
          Backend.Matrix.Elementwise.elementwiseAddAtLeastOneToCOOTests
          Backend.Matrix.Mxm.tests
          Backend.Matrix.Transpose.tests ]
        |> testSequenced

[<Tests>]
let commonTests =
    testList
        "Common tests"
        [ Backend.Common.BitonicSort.tests
          Backend.Common.PrefixSum.tests
          Backend.Common.Scatter.tests
          Backend.Common.RemoveDuplicates.tests
          Backend.Common.Copy.tests
          Backend.Common.Replicate.tests
          Backend.Common.Reduce.tests
          Backend.Common.Sum.tests ]
        |> testSequenced

let vectorTests =
    testList
        "Vector tests"
        [ Backend.Vector.SpMV.tests
          Backend.Vector.ZeroCreate.tests
          Backend.Vector.OfList.tests
          Backend.Vector.Copy.tests
          Backend.Vector.Convert.tests
          Backend.Vector.ElementWiseAtLeastOne.addTests
          Backend.Vector.ElementWiseAtLeastOne.mulTests
          Backend.Vector.ElementWise.addTests
          Backend.Vector.ElementWise.mulTests
          Backend.Vector.FillSubVector.tests
          Backend.Vector.FillSubVector.complementedTests
          Backend.Vector.Reduce.tests
          Backend.Vector.ContainNonZero.tests ]
        |> testSequenced

let algoTests =
    testList
        "Algorithms tests"
        [ Backend.Algo.BFS.tests ]
        |> testSequenced

[<Tests>]
let allTests =
    testList
        "All tests"
        [ commonTests
          matrixTests
          vectorTests
          algoTests ]
    |> testSequenced

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [] argv
