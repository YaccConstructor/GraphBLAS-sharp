namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation
open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic
open GraphBLAS.FSharp.Helpers

type GraphblasContext =
    {
        ClContext: OpenCLEvaluationContext
    }

type GraphblasEvaluation<'a> = EvalGB of (GraphblasContext -> 'a)

module EvalGB =
    let defaultEnv = { ClContext = OpenCLEvaluationContext() }

    let private runCl env (OpenCLEvaluation f) = f env

    let run env (EvalGB action) = action env

    let ask = EvalGB id

    let asks f = EvalGB f

    let bind f reader =
        EvalGB <| fun env ->
            let x = run env reader
            run env (f x)

    let (>>=) x f = bind f x

    let return' x =
        EvalGB <| fun _ -> x

    let returnFrom x = x

    let fromCl clEvaluation =
        EvalGB <| fun env ->
            runCl env.ClContext clEvaluation

    let withClContext clContext (EvalGB action) =
        ask >>= fun env ->
        return' ^ action { env with ClContext = clContext }

    let runSync (EvalGB action) =
        let result = action defaultEnv
        defaultEnv.ClContext.CommandQueue.Finish() |> ignore
        result

type GraphblasBuilder() =
    member this.Bind(x, f) = EvalGB.bind f x
    member this.Return x = EvalGB.return' x
    member this.ReturnFrom x = x

    member this.Zero() =
        EvalGB.return' ()

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

    member this.TryWith(tryBlock, handler) =
        EvalGB <| fun env ->
            try
                EvalGB.run env tryBlock
            with
            | e ->
                EvalGB.run env (handler e)

[<AutoOpen>]
module GraphblasBuilder =
    let graphblas = GraphblasBuilder()

    let (>>=) x f = EvalGB.bind f x
