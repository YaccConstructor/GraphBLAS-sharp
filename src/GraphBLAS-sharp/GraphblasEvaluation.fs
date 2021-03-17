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

    let ret x =
        EvalGB <| fun _ -> x

    // EvalGB.liftCl x === liftM EvalGB x
    let liftCl clEvaluation =
        EvalGB <| fun env ->
            runCl env.ClContext clEvaluation

    let runWithClContext clContext (EvalGB action) =
        action { ClContext = clContext }

type GraphblasBuilder() =
    member this.Bind(x, f) = EvalGB.bind f x
    member this.Return x = EvalGB.ret x
    member this.ReturnFrom x = x

    member this.Zero() =
        EvalGB.ret ()

    member this.Combine(m1, m2) =
        EvalGB <| fun env ->
            EvalGB.run env m1
            EvalGB.run env m2

    member this.Delay rest =
        EvalGB <| fun env ->
            EvalGB.run env <| rest ()

    member this.While(predicate, body) =
        EvalGB <| fun env ->
            while predicate () do
                EvalGB.run env body

    member this.For(sequence, f) =
        EvalGB <| fun env ->
            for elem in sequence do
                EvalGB.run env (f elem)

[<AutoOpen>]
module GraphblasBuilder =
    let graphblas = GraphblasBuilder()