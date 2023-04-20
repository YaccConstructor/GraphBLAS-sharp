namespace GraphBLAS.FSharp.Backend.Quotes

open GraphBLAS.FSharp.Backend.Objects

module ArithmeticOperations =
    // unary
    let inline optionUnOp zero unaryOp =
        <@ fun x ->
            let mutable res = zero

            match x with
            | Some v -> res <- (%unaryOp) v
            | None -> res <- (%unaryOp) zero

            if res = zero then None else Some res @>

    let inline addLeftConst zero constant =
        optionUnOp zero <@ fun x -> constant + x @>

    let inline addRightConst zero constant =
        optionUnOp zero <@ fun x -> x + constant @>

    let inline mulLeftConst zero constant =
        optionUnOp zero <@ fun x -> constant * x @>

    let inline mulRightConst zero constant =
        optionUnOp zero <@ fun x -> x * constant @>

    // binary

    let inline optionBinOpQ zero binOp =
        <@ fun (x: 'a option) (y: 'a option) ->
            let mutable res = zero

            match x, y with
            | Some f, Some s -> res <- (%binOp) f s
            | Some f, None -> res <- f
            | None, Some s -> res <- s
            | None, None -> ()

            if res = zero then None else Some res @>

    let inline optionBinOp zero binOp =
        fun (x: 'a option) (y: 'a option) ->
            let mutable res = zero

            match x, y with
            | Some left, Some right -> res <- binOp left right
            | Some left, None -> res <- left
            | None, Some right -> res <- right
            | None, None -> ()

            if res = zero then None else Some res

    let createOptionPair zero opQ op =
        optionBinOpQ zero opQ, optionBinOp zero op

    let inline createOptionSumPair zero = createOptionPair zero <@ (+) @> (+)

    let intSumOption = createOptionSumPair 0
    let byteSumOption = createOptionSumPair 0uy
    let floatSumOption = createOptionSumPair 0.0
    let float32SumOption = createOptionSumPair 0f

    let boolSumOption = createOptionPair false <@ (||) @> (||)

    let inline createOptionMulPair zero = createOptionPair zero <@ (*) @> (*)

    let intMulOption = createOptionMulPair 0
    let byteMulOption = createOptionMulPair 0uy
    let floatMulOption = createOptionMulPair 0.0
    let float32MulOption = createOptionMulPair 0f

    let boolMulOption = createOptionPair true <@ (&&) @> (&&)

    let inline atLeastOneBinOpQ zero binOp =
        Convert.optionToAtLeastOne <| optionBinOpQ zero binOp

    let inline atLeastOneBinOp zero binOp =
        let optionOp = optionBinOp zero binOp
        // convert AtLeastOne -> Option
        function
        | Both (left, right) -> optionOp (Some left) (Some right)
        | Left left -> optionOp (Some left) None
        | Right right -> optionOp None (Some right)

    let inline createAtLeastOnePair zero opQ op =
        atLeastOneBinOpQ zero opQ, atLeastOneBinOp zero op

    let inline createAtLeastOneSumPair zero = createAtLeastOnePair zero <@ (+) @> (+)

    let intSumAtLeastOne = createAtLeastOneSumPair 0
    let byteSumAtLeastOne = createAtLeastOneSumPair 0uy
    let floatSumAtLeastOne = createAtLeastOneSumPair 0.0
    let float32SumAtLeastOne = createAtLeastOneSumPair 0f

    let boolSumAtLeastOne = createAtLeastOnePair false <@ (||) @> (||)

    let inline createAtLeastOneMulPair zero = createAtLeastOnePair zero <@ (*) @> (*)

    let intMulAtLeastOne = createAtLeastOneMulPair 0
    let byteMulAtLeastOne = createAtLeastOneMulPair 0uy
    let floatMulAtLeastOne = createAtLeastOneMulPair 0.0
    let float32MulAtLeastOne = createAtLeastOneMulPair 0f

    let boolMulAtLeastOne = createAtLeastOnePair true <@ (&&) @> (&&)

    let notOption =
        <@ fun x ->
            match x with
            | Some true -> None
            | _ -> Some true @>

    // unwrapped operands
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
