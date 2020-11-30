module Tests

open System
open Expecto
open GraphBLAS.FSharp
open Brahma.OpenCL
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open FSharp.Quotations

let ctx = OpenCLEvaluationContext("INTEL*")

let GpuMap (f: Expr<'a -> 'b>) (input : array<'a>) =
    opencl {
        let xs = Array.zeroCreate input.Length

        let code =
            <@
                fun (range : _1D) (input : array<'a>) (output : array<'b>) ->
                    let idx = range.GlobalID0
                    output.[idx] <- (%f) input.[idx]
            @>

        let binder kernelP =
            let range = _1D <| input.Length
            kernelP range input xs

        do! RunCommand code binder
        return xs
    }

[<Tests>]
let tests =
    testList "samples" [
        testCase "Test 1" <| fun _ ->
            let xs = [|1; 2; 3; 4|]
            let workflow =
                opencl {
                    let! ys = GpuMap <@ fun x -> x * x + 10 @> xs
                    let! zs = GpuMap <@ fun x -> x + 1 @> ys
                    return! ToHost zs
                }
            let output = ctx.RunSync workflow
            Expect.equal output [|12; 15; 20; 27|] "kek"
    ]
