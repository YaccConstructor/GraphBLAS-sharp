namespace GraphBLAS.FSharp.Tests

open FsCheck
open System
open GraphBLAS.FSharp
open Microsoft.FSharp.Reflection

type MaskType =
    | Regular
    | Complemented
    | None

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
    let rec cartesian listOfLists =
        match listOfLists with
        | [x] -> List.fold (fun acc elem -> [elem]::acc) [] x
        | h::t ->
            List.fold (fun cacc celem ->
                (List.fold (fun acc elem -> (elem::celem)::acc) [] h) @ cacc
            ) [] (cartesian t)
        | _ -> []

    let listOfUnionCases<'a> =
        FSharpType.GetUnionCases typeof<'a>
        |> Array.map (fun caseInfo -> FSharpValue.MakeUnion(caseInfo, [||]) :?> 'a)
        |> List.ofArray
