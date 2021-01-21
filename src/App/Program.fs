open System
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined
open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

[<EntryPoint>]
let main argv =

    // let leftVector = SparseVector<float>(30, [| 0;2;4;6;8;10;12;14 |], [| 4.3; 5.5; 32.4; 56.43; 54.67; 563.43; 765.43; 23.43 |])
    // let rightVector = SparseVector<float>(30, [| 1;3;5;7;9;11;13;15 |], [| -4.3; -5.5; -32.4; -0.43; 32.4; 56.43; 54.67; 563.43 |])

    let leftVector = SparseVector<int>(100, [| 1;2;3;4;5 |], [| 5; 32; 56; 54; 563 |])
    let rightVector = SparseVector<int>(100, [| 1;4;5;6;7;8;9 |], [| 32; -54; 54; 563; 563; 765; 23 |])

    // let command =
    //     <@
    //         fun (ndRange: _1D)
    //             (array: int[]) ->

    //             let i = ndRange.GlobalID0
    //             array.[i] <- if i % 2 = 0 then 42 else 43
    //     @>

    // let wf =
    //     opencl {
    //         let xs = [| 0 .. 10 |]
    //         let binder kernelP =
    //             let ndRange = _1D(xs.Length)
    //             kernelP
    //                 ndRange
    //                 xs
    //         do! RunCommand command binder
    //         return! ToHost xs
    //     }

    // let ys = oclContext.RunSync wf
    // printf "ys:"
    // for i in 0 .. ys.Length - 1 do
    //     printf "%i " ys.[i]
    // printfn ""

    let workflow =
        opencl {
            let! resultVector = leftVector.EWiseAdd rightVector None IntegerSemiring.addMult
            return! resultVector.ToHost()
        }

    let resultVector : SparseVector<int> = downcast oclContext.RunSync workflow

    let indices = resultVector.Indices
    let values = resultVector.Values

    for i in 0 .. indices.Length - 1 do
        printfn "(%i, %A)" indices.[i] values.[i]

    0
