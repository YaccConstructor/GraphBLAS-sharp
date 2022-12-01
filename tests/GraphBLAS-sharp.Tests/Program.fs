open Expecto

open Brahma.FSharp.OpenCL
open GraphBLAS.FSharp.Backend.Common
open OpenCL.Net
open GraphBLAS.FSharp
//open GraphBLAS.FSharp.Algorithms
open GraphBLAS.FSharp.IO

[<Tests>]
let allTests =
    testList
        "All tests"
        [ Backend.Mxm.tests
          Backend.BitonicSort.tests
          Backend.PrefixSum.tests
          Backend.Scatter.tests
          Backend.Convert.tests
          Backend.RemoveDuplicates.tests
          Backend.Copy.tests
          Backend.Replicate.tests
          //Backend.Elementwise.elementwiseAddTests
          //Backend.Elementwise.elementwiseAddAtLeastOneTests
          //Backend.Elementwise.elementwiseAddAtLeastOneToCOOTests
          //Backend.Elementwise.elementwiseMulAtLeastOneTests
          Backend.Transpose.tests
          Backend.SpMV.tests
          //Matrix.GetTuples.tests
          //Matrix.Mxv.tests
          //Algo.Bfs.tests
          Backend.Reduce.tests
          Backend.Sum.tests
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

[<EntryPoint>]
let main argv = allTests |> runTestsWithCLIArgs [] argv

// graphblas {
//     let! matrix =
//         MtxReader("webbase-1M.mtx").ReadMatrix(fun _ -> 1)
//         |> Matrix.switch CSR
//         >>= Matrix.synchronizeAndReturn
//     let! vector = Vector.ofList matrix.ColumnCount (List.init matrix.ColumnCount (fun i -> i, 1))
//     return!
//         Matrix.mxv Predefined.AddMult.int matrix vector
//         >>= Vector.synchronizeAndReturn
// }
// |> EvalGB.withClContext (OpenCLEvaluationContext())
// |> EvalGB.runSync
// |> printfn "%A"

// 0
