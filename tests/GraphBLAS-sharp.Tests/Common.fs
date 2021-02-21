namespace GraphBLAS.FSharp.Tests

open FsCheck
open System

type VectorType =
    | Sparse = 0
    | Dense = 1

type MatrixType =
    | CSR = 0
    | COO = 1

type MaskType =
    | Regular = 0
    | Complemented = 1
    | None = 2

type OperationCase = {
    VectorCase: VectorType
    MatrixCase: MatrixType
    MaskCase: MaskType
}

module Generators =
    let dimension2DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (0, size |> float |> sqrt |> int)
            |> Gen.two

    let dimension3DGenerator =
        Gen.sized <| fun size ->
            Gen.choose (0, size |> float |> sqrt |> int)
            |> Gen.three

    // non-empty and nonzeroes exists
    let pairOfSparseMatricesGenerator (valuesGenerator: Gen<'a>) (zero: 'a) (checkZero: 'a -> bool) =
        Gen.sized <| fun size ->
            let sparseGenerator =
                Gen.oneof [
                    valuesGenerator
                    Gen.constant zero
                ]

            gen {
                let! (rowsA, colsA, colsB) = dimension3DGenerator
                let! matrixA = valuesGenerator |> Gen.array2DOfDim (rowsA, colsA)
                let! matrixB = valuesGenerator |> Gen.array2DOfDim (colsA, colsB)
                return (matrixA, matrixB)
            }
            |> Gen.filter (fun (matrixA, matrixB) -> matrixA.Length <> 0 && matrixB.Length <> 0)
            |> Gen.filter
                (fun (matrixA, matrixB) ->
                    matrixA |> Seq.cast<'a> |> Seq.exists (not << checkZero) &&
                    matrixB |> Seq.cast<'a> |> Seq.exists (not << checkZero)
                )

module Utils =
    let rec cartesian lstlst =
        match lstlst with
        | [x] ->
            List.fold (fun acc elem -> [elem]::acc) [] x
        | h::t ->
            List.fold (fun cacc celem ->
                (List.fold (fun acc elem -> (elem::celem)::acc) [] h) @ cacc
                ) [] (cartesian t)
        | _ -> []

    /// Return all values for an enumeration type
    let enumValues (enumType: Type) : int list =
        let values = Enum.GetValues enumType
        let lb = values.GetLowerBound 0
        let ub = values.GetUpperBound 0
        [lb .. ub] |> List.map (fun i -> values.GetValue i :?> int)
