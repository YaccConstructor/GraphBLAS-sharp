
open System
open GraphBLAS.FSharp

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open GlobalContext
open Helpers
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Predefined

[<EntryPoint>]
let main argv =
    let fstMatrix = COOMatrix(100, 100, [|0;1;2|], [|0;1;2|], [|1.;2.;3.|])
    let sndMatrix = COOMatrix(100, 100, [|0;1;2|], [|0;1;2|], [|1.;2.;3.|])
    let workflow =
        opencl {
            let! result = fstMatrix.EWiseAdd sndMatrix None FloatSemiring.addMult
            let! _ = result.ToHost ()
            return result
        }
    let res: COOMatrix<float> = downcast oclContext.RunSync workflow

    let indices = res.Indices
    let values = res.Values

    for i in 0 .. indices.Length - 1 do
        let index = indices.[i]
        let i, j = int <| index >>> 32, int index
        printfn "(%i, %i, %A)" i j values.[i]

    0
