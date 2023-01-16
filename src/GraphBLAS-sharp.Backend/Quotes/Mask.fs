namespace GraphBLAS.FSharp.Backend.Quotes

module Mask =
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
