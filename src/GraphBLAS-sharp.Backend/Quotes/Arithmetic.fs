namespace GraphBLAS.FSharp.Backend.Quotes

open GraphBLAS.FSharp.Objects

module ArithmeticOperations =
    let inline mkUnaryOp zero unaryOp =
        <@ fun x ->
            let mutable res = zero

            match x with
            | Some v -> res <- (%unaryOp) v
            | None -> res <- (%unaryOp) zero

            if res = zero then None else Some res @>

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

    let byteSumOption =
        <@ fun (x: byte option) (y: byte option) ->
            let mutable res = 0

            // Converted to int because of Quotations Evaluator issue.
            let xInt =
                match x with
                | Some x -> Some(int x)
                | None -> None

            let yInt =
                match y with
                | Some y -> Some(int y)
                | None -> None

            match xInt, yInt with
            | Some f, Some s -> res <- f + s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            let byteRes = byte res

            if byteRes = 0uy then
                None
            else
                Some byteRes @>

    let boolSumOption =
        <@ fun (x: bool option) (y: bool option) ->
            let mutable res = false

            match x, y with
            | None, None -> ()
            | _ -> res <- true

            if res then Some true else None @>

    let inline addLeftConst zero constant =
        mkUnaryOp zero <@ fun x -> constant + x @>

    let inline addRightConst zero constant =
        mkUnaryOp zero <@ fun x -> x + constant @>

    let intSumOption = mkNumericSum 0
    let floatSumOption = mkNumericSum 0.0
    let float32SumOption = mkNumericSum 0f

    let boolSumAtLeastOne =
        <@ fun (_: AtLeastOne<bool, bool>) -> Some true @>

    let intSumAtLeastOne = mkNumericSumAtLeastOne 0
    let byteSumAtLeastOne = mkNumericSumAtLeastOne 0uy
    let floatSumAtLeastOne = mkNumericSumAtLeastOne 0.0
    let float32SumAtLeastOne = mkNumericSumAtLeastOne 0f

    let byteMulOption =
        <@ fun (x: byte option) (y: byte option) ->
            let mutable res = 0

            // Converted to int because of Quotations Evaluator issue.
            let xInt =
                match x with
                | Some x -> Some(int x)
                | None -> None

            let yInt =
                match y with
                | Some y -> Some(int y)
                | None -> None

            match xInt, yInt with
            | Some f, Some s -> res <- f * s
            | _ -> ()

            let byteRes = byte res

            if byteRes = 0uy then
                None
            else
                Some byteRes @>

    let boolMulOption =
        <@ fun (x: bool option) (y: bool option) ->
            let mutable res = false

            match x, y with
            | Some _, Some _ -> res <- true
            | _ -> ()

            if res then Some true else None @>

    let inline mulLeftConst zero constant =
        mkUnaryOp zero <@ fun x -> constant * x @>

    let inline mulRightConst zero constant =
        mkUnaryOp zero <@ fun x -> x * constant @>

    let intMulOption = mkNumericMul 0
    let floatMulOption = mkNumericMul 0.0
    let float32MulOption = mkNumericMul 0f

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

    let notOption =
        <@ fun x ->
            match x with
            | Some true -> None
            | _ -> Some true @>

    let inline private binOpQ zero op =
        <@ fun (left: 'a) (right: 'a) ->
            let result = (%op) left right

            if result = zero then
                None
            else
                Some result @>

    let inline private binOp zero op =
        fun left right ->
            let result = op left right

            if result = zero then
                None
            else
                Some result

    let inline createPair zero op opQ = binOpQ zero opQ, binOp zero op

    // addition
    let intAdd = createPair 0 (+) <@ (+) @>

    let boolAdd = createPair false (||) <@ (||) @>

    let floatAdd = createPair 0.0 (+) <@ (+) @>

    let float32Add = createPair 0.0f (+) <@ (+) @>

    // multiplication
    let intMul = createPair 0 (*) <@ (*) @>

    let boolMul = createPair true (&&) <@ (&&) @>

    let floatMul = createPair 0.0 (*) <@ (*) @>

    let float32Mul = createPair 0.0f (*) <@ (*) @>
