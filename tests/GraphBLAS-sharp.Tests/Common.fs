namespace GraphBLAS.FSharp.Tests

open FsCheck
open System
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection
open Brahma.FSharp.OpenCL.WorkflowBuilder.Evaluation

type MatrixBackendFormat =
    | CSR
    | COO

type VectorBackendFormat =
    | Sparse
    | Dense

type MaskType =
    | Regular
    | Complemented
    | NoMask

module BackendState =
    let mutable oclContext = OpenCLEvaluationContext()
    let mutable matrixBackendFormat = CSR

module Generators =
    let dimension2DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (1, size |> float |> sqrt |> int)
            |> Gen.two

    let dimension3DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (1, size |> float |> sqrt |> int)
            |> Gen.three

    // non-empty and nonzeroes exists
    let pairOfSparseMatricesGenerator (valuesGenerator: Gen<'a>) (zero: 'a) (isZero: 'a -> bool) =
        Gen.sized <| fun size ->
            let sparseGenerator =
                Gen.oneof [
                    valuesGenerator
                    Gen.constant zero
                ]

            gen {
                // let! (rowsA, colsA, colsB) = dimension3DGenerator
                let! (nrows, ncols) = dimension2DGenerator
                let! matrixA = sparseGenerator |> Gen.array2DOfDim (nrows, ncols)
                let! matrixB = sparseGenerator |> Gen.array2DOfDim (nrows, ncols)
                return (matrixA, matrixB)
            }
            |> Gen.filter (fun (matrixA, matrixB) -> matrixA.Length <> 0 && matrixB.Length <> 0)
            |> Gen.filter
                (fun (matrixA, matrixB) ->
                    matrixA |> Seq.cast<'a> |> Seq.exists (not << isZero) &&
                    matrixB |> Seq.cast<'a> |> Seq.exists (not << isZero)
                )

module Utils =
    let rec cartesian listOfLists =
        match listOfLists with
        | [x] -> List.fold (fun acc elem -> [elem] :: acc) [] x
        | h :: t ->
            List.fold (fun cacc celem ->
                (List.fold (fun acc elem -> (elem :: celem) :: acc) [] h) @ cacc
            ) [] (cartesian t)
        | _ -> []

    let listOfUnionCases<'a> =
        FSharpType.GetUnionCases typeof<'a>
        |> Array.map (fun caseInfo -> FSharpValue.MakeUnion(caseInfo, [||]) :?> 'a)
        |> List.ofArray
