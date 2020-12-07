namespace GraphBLAS.FSharp.Tests

open System

type VectorType =
    | Sparse = 0
    | Dense = 1

type MatrixType =
    | CSR = 0
    | COO = 1
    | Dense = 2

type MaskType =
    | Regular = 0
    | Complemented = 1
    | None = 2

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
