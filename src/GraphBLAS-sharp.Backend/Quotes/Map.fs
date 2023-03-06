namespace GraphBLAS.FSharp.Backend.Quotes

open FSharp.Quotations

module Map =
    let optionToValueOrZero<'a> zero =
        <@ fun (item: 'a option) ->
            match item with
            | Some value -> value
            | None -> zero @>

    let option onSome onNone =
        <@ function
        | Some _ -> onSome
        | None -> onNone @>

    let id<'a> = <@ fun (item: 'a) -> item @>

    let chooseBitmap<'a, 'b> (map: Expr<'a -> 'b option>) =
        <@ fun (item: 'a) ->
            match (%map) item with
            | Some _ -> 1
            | None -> 0 @>
