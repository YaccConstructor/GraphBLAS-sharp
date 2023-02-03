namespace GraphBLAS.FSharp.Backend.Quotes

open FSharp.Quotations
open GraphBLAS.FSharp.Backend.Objects

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

    let map2WithValueToMap2 (op: Expr<'a option -> 'b option -> 'c option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (_: 'a option) -> (%op) leftItem rightItem @>

    let map2WithValueToAssignByMask (op: Expr<'a option -> 'b option -> 'c option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (optionValue: 'b option) ->
            match rightItem with
            | Some _ -> (%op) leftItem optionValue
            | None -> (%op) leftItem None @>

    let map2WithValueToAssignByMaskComplemented (op: Expr<'a option -> 'b option -> 'c option>) =
        <@ fun (leftItem: 'a option) (rightItem: 'b option) (optionValue: 'b option) ->
            match rightItem with
            | Some _ -> (%op) leftItem None
            | None -> (%op) leftItem optionValue @>
