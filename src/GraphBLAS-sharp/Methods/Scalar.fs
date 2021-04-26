namespace GraphBLAS.FSharp

open Brahma.FSharp.OpenCL.WorkflowBuilder.Basic

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Scalar =

    (*
        constructors
    *)

    let internal createFromArray (value: 'a[]) : GraphblasEvaluation<Scalar<'a>> = graphblas { return Scalar.FromArray(value) }

    let create (value: 'a) : GraphblasEvaluation<Scalar<'a>> = createFromArray [|value|]

    (*
        methods
    *)

    let copy (scalar: Scalar<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"

    let synchronize (scalar: Scalar<'a>) : GraphblasEvaluation<unit> =
        opencl {
            let! _ = ToHost scalar.Value

            return ()
        }
        |> EvalGB.fromCl

    (*
        assignment and extraction
    *)

    let extractValue (scalar: Scalar<'a>) : GraphblasEvaluation<'a> = graphblas { return scalar.Value.[0] }

    let internal extractArray (scalar: Scalar<'a>) : GraphblasEvaluation<'a[]> = graphblas { return scalar.Value }

    let assignValue (scalar: Scalar<'a>) (target: Scalar<'a>) : GraphblasEvaluation<unit> =
        failwith "Not Implemented yet"
