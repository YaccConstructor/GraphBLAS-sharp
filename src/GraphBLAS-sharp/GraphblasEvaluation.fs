namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

type GraphblasContext =
    {
        ClContext: OpenCLEvaluationContext
    }

type GraphblasEvaluation<'a> = EvalGB of (GraphblasContext -> 'a)

module EvalGB =
    let private runCl env (OpenCLEvaluation f) = f env

    let run env (EvalGB action) = action env

    let ask = EvalGB id

    let asks f = EvalGB f

    let bind f reader =
        EvalGB <| fun env ->
            let x = run env reader
            run env (f x)

    // EvalGB.liftCl x === liftM EvalGB x
    let liftCl clEvaluation =
        EvalGB <| fun env ->
            clEvaluation
            |> runCl env.ClContext

type GraphblasBuilder() =
    // monad
    member this.Bind(x, f) = EvalGB.bind f x
    member this.Return x = EvalGB <| fun _ -> x
    member this.ReturnFrom x = x

[<AutoOpen>]
module GraphblasBuilder =
    let graphblas = GraphblasBuilder()
