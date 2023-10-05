namespace GraphBLAS.FSharp.Backend.Quotes

open FSharp.Quotations
open GraphBLAS.FSharp.Objects

module Convert =
    let atLeastOneToOption op =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) ->
            match leftItem, rightItem with
            | Some left, Some right -> (%op) (Both(left, right))
            | None, Some right -> (%op) (Right right)
            | Some left, None -> (%op) (Left left)
            | None, None -> None @>

    let assignToOption (op: Expr<'a option -> 'a option -> 'a option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (value: 'a) ->
            match rightItem with
            | Some _ -> (%op) leftItem (Some value)
            | None -> (%op) leftItem None @>

    let assignComplementedToOption (op: Expr<'a option -> 'a option -> 'a option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (value: 'a) ->
            match rightItem with
            | Some _ -> (%op) leftItem None
            | None -> (%op) leftItem (Some value) @>

    let map2ToMapLeftNone (op: Expr<'a option -> 'b option -> 'c option>) =
        <@ fun rightItem -> (%op) None rightItem @>

    let map2ToMapRightNone (op: Expr<'a option -> 'b option -> 'c option>) =
        <@ fun leftItem -> (%op) leftItem None @>

    let map2ToNoneNone (op: Expr<'a option -> 'b option -> 'c option>) = <@ (%op) None None @>
