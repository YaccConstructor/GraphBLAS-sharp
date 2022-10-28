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
        [
        // [ Backend.BitonicSort.tests //TODO()
        //   Backend.PrefixSum.tests
        //   Backend.Convert.tests
        //   Backend.RemoveDuplicates.tests
        //   Backend.Copy.tests
        //   Backend.Replicate.tests
        //   Backend.EwiseAdd.tests
        //   Backend.EwiseAdd.tests2 //TODO()
        //   //Backend.EwiseAdd.tests3
        //   Backend.Transpose.tests //TODO()
        //   //Matrix.GetTuples.tests
        //   //Matrix.Mxv.tests
        //   //Algo.Bfs.tests
        //Backend.Vector.ZeroCreate.tests //TODO()
        //Backend.Reduce.tests
        //Backend.Vector.ZeroCreate.tests
        //Backend.Vector.OfList.tests
        //Backend.Vector.Copy.tests
        //Backend.Vector.Convert.tests
        //Backend.Vector.FillSubVector.tests
        // Backend.Vector.ElementWiseAddAtLeastOne.addTests
        // Backend.Vector.ElementWiseAddAtLeastOne.mulTests
        //Backend.Vector.FillSubVector.tests
        //Backend.Vector.Complemented.tests
          Backend.Vector.Reduce.tests
          ]
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
