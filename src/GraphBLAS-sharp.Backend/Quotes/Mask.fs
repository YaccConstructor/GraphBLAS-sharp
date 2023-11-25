namespace GraphBLAS.FSharp.Backend.Quotes

module Mask =
    let assign<'a when 'a: struct> =
        <@ fun (left: 'a option) (right: 'a option) ->
            match left, right with
            | _, None -> left
            | _ -> right @>

    let op<'a, 'b when 'a: struct and 'b: struct> =
        <@ fun (left: 'a option) (right: 'b option) ->
            match right with
            | Some _ -> left
            | _ -> None @>

    let complementedOp<'a, 'b when 'a: struct and 'b: struct> =
        <@ fun (left: 'a option) (right: 'b option) ->
            match right with
            | None -> left
            | _ -> None @>
