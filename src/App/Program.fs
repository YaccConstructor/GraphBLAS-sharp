
open Brahma.OpenCL
open Brahma.FSharp.OpenCL.Core
open Brahma.FSharp.OpenCL.Extensions
open FSharp.Quotations.Evaluator
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open GraphBLAS.FSharp
open GraphBLAS.FSharp.Predefined

[<EntryPoint>]
let main argv =
    let fstMatrix = COOMatrix(100, 100, [|0;1;1;2;5|], [|4;1;50;2;5|], [|1.0;2.0;76.0;3.0;6.0|])
    let sndMatrix = COOMatrix(100, 100, [|0;1;1;2;5|], [|4;2;50;3;5|], [|-0.8;2.0;-76.0;3.0;6.0|])
    let workflow =
        opencl {
            let! result = fstMatrix.EWiseAdd sndMatrix None FloatSemiring.addMult
            let! _ = result.ToHost ()
            return result
        }
    let res: COOMatrix<float> = downcast oclContext.RunSync workflow

    let indices = res.Indices
    // let rows = res.Rows
    // let columns = res.Columns
    let values = res.Values

    // for i in 0 .. rows.Length - 1 do
    //     printfn "(%i, %i, %A)" rows.[i] columns.[i] values.[i]

    for i in 0 .. indices.Length - 1 do
        let index = indices.[i]
        let a = int (index >>> 32)
        let b = int index
        printfn "(%i, %i, %A)" a b values.[i]

    // let allIndicesLength = 42
    // let workGroupSize = 256
    // let longSide = 72
    // let shortSide = 27

    // let command =
    //     <@
    //         fun (ndRange: _1D)
    //             (arr: int[]) ->

    //             arr.[ndRange.GlobalID0] <-
    //                 if true then 41 else 42
    //     @>

    // let translate2opencl (provider: ComputeProvider) (command: Quotations.Expr<(_1D -> int[] -> unit)>) : string =
    //     let options = ComputeProvider.DefaultOptions_p
    //     let tOptions = []
    //     provider.SetCompileOptions options

    //     let kernel = System.Activator.CreateInstance<Kernel<_1D>>()
    //     CLCodeGenerator.GenerateKernel(command, provider, kernel, tOptions) |> ignore
    //     let code = (kernel :> ICLKernel).Source.ToString()
    //     code

    // let code = translate2opencl oclContext.Provider command

    // printf "%s" code

    0
