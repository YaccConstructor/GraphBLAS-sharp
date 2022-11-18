namespace GraphBLAS.FSharp.Backend.Common

open FSharp.Quotations

type AtLeastOne<'a, 'b when 'a: struct and 'b: struct> =
    | Both of 'a * 'b
    | Left of 'a
    | Right of 'b

module StandardOperations =
    let inline mkNumericSum zero =
        <@ fun (x: 't option) (y: 't option) ->
            let mutable res = zero

            match x, y with
            | Some f, Some s -> res <- f + s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            if res = zero then None else Some res @>

    let inline mkNumericSumAtLeastOne zero =
        <@ fun (values: AtLeastOne<'t, 't>) ->
            let mutable res = zero

            match values with
            | Both (f, s) -> res <- f + s
            | Left f -> res <- f
            | Right s -> res <- s

            if res = zero then None else Some res @>

    let inline mkNumericMul zero =
        <@ fun (x: 't option) (y: 't option) ->
            let mutable res = zero

            match x, y with
            | Some f, Some s -> res <- f * s
            | _ -> ()

            if res = zero then None else Some res @>

    let inline mkNumericMulAtLeastOne zero =
        <@ fun (values: AtLeastOne<'t, 't>) ->
            let mutable res = zero

            match values with
            | Both (f, s) -> res <- f * s
            | _ -> ()

            if res = zero then None else Some res @>

    let boolSum =
        <@ fun (x: bool option) (y: bool option) ->
            let mutable res = false

            match x, y with
            | None, None -> ()
            | _ -> res <- true

            if res then Some true else None @>

    let intSum = mkNumericSum 0
    let byteSum = mkNumericSum 0uy
    let floatSum = mkNumericSum 0.0
    let float32Sum = mkNumericSum 0f

    let boolSumAtLeastOne =
        <@ fun (_: AtLeastOne<bool, bool>) -> Some true @>

    let intSumAtLeastOne = mkNumericSumAtLeastOne 0
    let byteSumAtLeastOne = mkNumericSumAtLeastOne 0uy
    let floatSumAtLeastOne = mkNumericSumAtLeastOne 0.0
    let float32SumAtLeastOne = mkNumericSumAtLeastOne 0f

    let boolMul =
        <@ fun (x: bool option) (y: bool option) ->
            let mutable res = false

            match x, y with
            | Some _, Some _ -> res <- true
            | _ -> ()

            if res then Some true else None @>

    let intMul = mkNumericMul 0
    let byteMul = mkNumericMul 0uy
    let floatMul = mkNumericMul 0.0
    let float32Mul = mkNumericMul 0f

    let boolMulAtLeastOne =
        <@ fun (values: AtLeastOne<bool, bool>) ->
            let mutable res = false

            match values with
            | Both _ -> res <- true
            | _ -> ()

            if res then Some true else None @>

    let intMulAtLeastOne = mkNumericMulAtLeastOne 0
    let byteMulAtLeastOne = mkNumericMulAtLeastOne 0uy
    let floatMulAtLeastOne = mkNumericMulAtLeastOne 0.0
    let float32MulAtLeastOne = mkNumericMulAtLeastOne 0f

    let atLeastOneToOption op =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) ->
            match leftItem, rightItem with
            | Some left, Some right -> (%op) (Both(left, right))
            | None, Some right -> (%op) (Right right)
            | Some left, None -> (%op) (Left left)
            | None, None -> None @>

    let fillSubToOption (op: Expr<'a option -> 'a option -> 'a option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (value: 'a) ->
            match rightItem with
            | Some _ -> (%op) leftItem (Some value)
            | None -> (%op) leftItem None @>

    let fillSubComplementedToOption (op: Expr<'a option -> 'a option -> 'a option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (value: 'a) ->
            match rightItem with
            | Some _ -> (%op) leftItem None
            | None -> (%op) leftItem (Some value) @>

    let fillSubOp<'a when 'a: struct> =
        <@ fun (left: 'a option) (right: 'a option) ->
            match left, right with
            | _, None -> left
            | _ -> right @>

    let maskOp<'a, 'b when 'a: struct and 'b: struct> =
        <@ fun (left: 'a option) (right: 'b option) ->
            match right with
            | Some _ -> left
            | _ -> None @>

    let complementedMaskOp<'a, 'b when 'a: struct and 'b: struct> =
        <@ fun (left: 'a option) (right: 'b option) ->
            match right with
            | None -> left
            | _ -> None @>
