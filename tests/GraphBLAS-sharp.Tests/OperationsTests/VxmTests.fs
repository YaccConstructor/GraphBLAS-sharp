namespace GraphBLAS.FSharp.Tests

open Expecto
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

type OperationCase = {
    VectorCaseType: VectorType
    MatrixCaseType: MatrixType
    MaskCaseType: MaskType
}

module VxmTests =
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

    let testCases =
        [
            typeof<VectorType>
            typeof<MatrixType>
            typeof<MaskType>
        ]
        |> List.map enumValues
        |> cartesian
        |> List.map (fun list -> {
                VectorCaseType = enum<VectorType> list.[0]
                MatrixCaseType = enum<MatrixType> list.[1]
                MaskCaseType = enum<MaskType> list.[2]
            })

    let matrix =
        array2D [ [ 12; 0; 7 ]
                  [ 1; 0; 0 ]
                  [ 3; 6; 9 ] ]

    let vector = [| 1; 1; 0 |]

    [<Tests>]
    let testList =
        testList "aaa" (
            testCases
            |> List.collect (fun case ->
                    let a = 1 // определеям контекст
                    testParam case [
                        "2", fun value () -> Expect.equal a a "ss"
                    ] |> List.ofSeq
                )
        )

// member this.Baddimensions() = ()
// let corectness = ()
// let zerovalue = ()
