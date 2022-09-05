namespace GraphBLAS.FSharp
//
//open Brahma.FSharp
//
//[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
//module Scalar =
//
//    (*
//        constructors
//    *)
//
//    let create (value: 'a) : GraphblasEvaluation<Scalar<'a>> =
//        graphblas { return ScalarWrapped { Value = [| value |] } }
//
//    (*
//        methods
//    *)
//
//    let copy (scalar: Scalar<'a>) : GraphblasEvaluation<Scalar<'a>> = failwith "Not Implemented yet"
//
//    let synchronize (scalar: Scalar<'a>) : GraphblasEvaluation<unit> =
//        match scalar with
//        | ScalarWrapped scalar ->
//            opencl {
//                failwith "FIX ME!"
//                //let! _ = ToHost scalar.Value
//                return ()
//            }
//        |> EvalGB.fromCl
//
//    (*
//        assignment and extraction
//    *)
//
//    let exportValue (scalar: Scalar<'a>) : GraphblasEvaluation<'a> =
//        graphblas {
//            do! synchronize scalar
//
//            match scalar with
//            | ScalarWrapped scalar -> return scalar.Value.[0]
//        }
//
//    let assignValue (scalar: Scalar<'a>) (target: Scalar<'a>) : GraphblasEvaluation<unit> =
//        failwith "Not Implemented yet"
