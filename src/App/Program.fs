open System
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined
open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<EntryPoint>]
let main argv =

    // let leftVector = SparseVector<float>(30, [| 0;2;4;6;8;10;12;14 |], [| 4.3; 5.5; 32.4; 56.43; 54.67; 563.43; 765.43; 23.43 |])
    // let rightVector = SparseVector<float>(30, [| 1;3;5;7;9;11;13;15 |], [| -4.3; -5.5; -32.4; -0.43; 32.4; 56.43; 54.67; 563.43 |])

    // let leftTuples : VectorTuples<int> = { Indices = [||]; Values = [||] }
    // let rightTuples : VectorTuples<int> = { Indices = [||]; Values = [||] }

    // let leftVector = COOVector.FromTuples(100, [| 1;2;3;4;5 |], [| 5; 32; 56; 54; 563 |])
    // let rightVector = COOVector.FromTuples(100, [| 1;4;5;6;7;8;9 |], [| 32; -54; 54; 563; 563; 765; 23 |])

    let mutable oclContext = OpenCLEvaluationContext()

    let len = 16777217
    let vector = COOVector.FromTuples(len + 5, Array.init len id, Array.create len 1)

    let workflow = graphblas {
        let! scalar = Vector.reduce Add.int (VectorCOO vector)
        do! Scalar.synchronize scalar
        return! (Scalar.extractValue scalar)
    }

    let result =
        workflow
        |> EvalGB.withClContext oclContext
        |> EvalGB.runSync

    printfn "Result: %A" result

    // let indices = resultVector.Indices
    // let values = resultVector.Values

    // for i in 0 .. indices.Length - 1 do
    //     printfn "(%i, %A)" indices.[i] values.[i]

    0
