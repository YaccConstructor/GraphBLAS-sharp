open System

open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

open System
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp.Predefined

open GraphBLAS.FSharp

[<EntryPoint>]
let main argv =

    // let mutable oclContext = OpenCLEvaluationContext(platform_name = "*")

    // let xs = [|6|]
    // let command =
    //     <@
    //         fun (ndRange: _1D)
    //             (xs: int[]) ->

    //             xs.[ndRange.GlobalID0] <- 42
    //     @>
    // let wf =
    //     opencl {
    //         let binder k =
    //             let ndRange = _1D(xs.Length)
    //             k ndRange xs
    //         do! RunCommand command binder
    //         return! ToHost xs
    //     }

    // let res = oclContext.RunSync wf
    // printfn "%A" res

    let mutable oclContext = OpenCLEvaluationContext()

    let workGroupSize = 256
    let workSize n =
        let m = n - 1
        m - m % workGroupSize + workGroupSize

    let leftMatrix = COOMatrix(100, 100, [|0;1;2;3;4;7|], [|1;7;5;6;0;1|], [|1.;2.;-4.;4.;5.;6.|])
    let rightMatrix = COOMatrix(100, 100, [|0;0;1;2;3;4;7|], [|1;5;4;5;7;0;1|], [|1.;2.;-4.;4.;5.;6.;7.|])
    // let leftMatrix = COOMatrix(100, 100, [||], [||], [||])
    // let rightMatrix = COOMatrix(100, 100, [||], [||], [||])

    let workflow =
        opencl {
            let! resultMatrix = leftMatrix.EWiseAdd rightMatrix None FloatSemiring.addMult
            let! tuples = resultMatrix.GetTuples()
            return! tuples.ToHost()
            //return! resultMatrix.ToHost()
        }

    // let resultMatrix : COOMatrix<float> = downcast oclContext.RunSync workflow
    // printfn "%O" resultMatrix
    let res = oclContext.RunSync workflow
    printfn "%A" res.RowIndices
    printfn "%A" res.ColumnIndices
    printfn "%A" res.Values

    0
